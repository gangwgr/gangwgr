package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
)

const (
	githubAPIURL     = "https://api.github.com/graphql"
	defaultUsername  = "gangwgr"
	defaultOutputDir = "output"
)

type Config struct {
	Username   string
	Token      string
	OutputDir  string
	OutputFile string
	Verbose    bool
}

type StatsCard struct {
	TotalStars         int
	TotalCommits       int
	TotalPRs           int
	TotalIssues        int
	ContributedTo      int
	TotalContributions int
}

type GitHubResponse struct {
	Data struct {
		User struct {
			ContributionsCollection struct {
				TotalCommitContributions      int `json:"totalCommitContributions"`
				TotalIssueContributions       int `json:"totalIssueContributions"`
				TotalPullRequestContributions int `json:"totalPullRequestContributions"`
				ContributionCalendar          struct {
					TotalContributions int `json:"totalContributions"`
				} `json:"contributionCalendar"`
			} `json:"contributionsCollection"`
			RepositoriesContributedTo struct {
				TotalCount int `json:"totalCount"`
			} `json:"repositoriesContributedTo"`
			Repositories struct {
				Nodes []struct {
					StargazerCount int `json:"stargazerCount"`
				} `json:"nodes"`
			} `json:"repositories"`
		} `json:"user"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"errors"`
}

func main() {
	config := loadConfig()

	if config.Verbose {
		log.Printf("Starting stats card generation for user: %s\n", config.Username)
	}

	ctx := context.Background()

	stats, err := fetchStatsWithRetry(ctx, config)
	if err != nil {
		log.Fatalf("Failed to fetch stats: %v\n", err)
	}

	if config.Verbose {
		log.Printf("Stats fetched successfully: %+v\n", stats)
	}

	svg := generateStatsCardSVG(stats)

	if err := ensureOutputDir(config.OutputDir); err != nil {
		log.Fatalf("Failed to create output directory: %v\n", err)
	}

	outputPath := fmt.Sprintf("%s/%s", config.OutputDir, config.OutputFile)
	if err := os.WriteFile(outputPath, []byte(svg), 0644); err != nil {
		log.Fatalf("Failed to write SVG: %v\n", err)
	}

	fmt.Printf("Stats card generated successfully: %s\n", outputPath)
}

func loadConfig() *Config {
	config := &Config{
		Username:   getEnv("GITHUB_USERNAME", defaultUsername),
		Token:      os.Getenv("GITHUB_TOKEN"),
		OutputDir:  getEnv("OUTPUT_DIR", defaultOutputDir),
		OutputFile: getEnv("OUTPUT_FILE", "stats-card.svg"),
		Verbose:    getEnvBool("VERBOSE", false),
	}

	if config.Token == "" {
		log.Fatal("GITHUB_TOKEN environment variable is required")
	}

	return config
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if b, err := strconv.ParseBool(value); err == nil {
			return b
		}
	}
	return defaultValue
}

func ensureOutputDir(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return os.MkdirAll(dir, 0755)
	}
	return nil
}

func fetchStatsWithRetry(ctx context.Context, config *Config) (*StatsCard, error) {
	var stats *StatsCard

	exponentialBackoff := backoff.NewExponentialBackOff()
	exponentialBackoff.MaxElapsedTime = 2 * time.Minute

	operation := func() error {
		var err error
		stats, err = fetchStats(ctx, config)
		if err != nil {
			if config.Verbose {
				log.Printf("Fetch attempt failed: %v. Retrying...\n", err)
			}
			return err
		}
		return nil
	}

	if err := backoff.Retry(operation, exponentialBackoff); err != nil {
		return nil, fmt.Errorf("failed after retries: %w", err)
	}

	return stats, nil
}

func fetchStats(ctx context.Context, config *Config) (*StatsCard, error) {
	query := fmt.Sprintf(`{
		user(login: "%s") {
			contributionsCollection {
				totalCommitContributions
				totalIssueContributions
				totalPullRequestContributions
				contributionCalendar {
					totalContributions
				}
			}
			repositoriesContributedTo(first: 1, contributionTypes: [COMMIT, ISSUE, PULL_REQUEST]) {
				totalCount
			}
			repositories(first: 100, ownerAffiliations: OWNER) {
				nodes {
					stargazerCount
				}
			}
		}
	}`, config.Username)

	reqBody := map[string]string{"query": query}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal query: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", githubAPIURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+config.Token)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(respBody))
	}

	// Check for rate limiting
	if resp.Header.Get("X-RateLimit-Remaining") == "0" {
		resetTime := resp.Header.Get("X-RateLimit-Reset")
		return nil, fmt.Errorf("rate limit exceeded, resets at: %s", resetTime)
	}

	var result GitHubResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	// Check for GraphQL errors
	if len(result.Errors) > 0 {
		return nil, fmt.Errorf("GraphQL errors: %v", result.Errors)
	}

	totalStars := 0
	for _, repo := range result.Data.User.Repositories.Nodes {
		totalStars += repo.StargazerCount
	}

	return &StatsCard{
		TotalStars:         totalStars,
		TotalCommits:       result.Data.User.ContributionsCollection.TotalCommitContributions,
		TotalPRs:           result.Data.User.ContributionsCollection.TotalPullRequestContributions,
		TotalIssues:        result.Data.User.ContributionsCollection.TotalIssueContributions,
		ContributedTo:      result.Data.User.RepositoriesContributedTo.TotalCount,
		TotalContributions: result.Data.User.ContributionsCollection.ContributionCalendar.TotalContributions,
	}, nil
}

func generateStatsCardSVG(stats *StatsCard) string {
	return fmt.Sprintf(`<svg width="600" height="150" viewBox="0 0 600 150" fill="none" xmlns="http://www.w3.org/2000/svg" role="img" aria-labelledby="descId">
  <title id="titleId">GitHub Stats Card</title>
  <desc id="descId">GitHub statistics for user contributions</desc>

  <style>
    .header { font: 600 18px 'Segoe UI', Ubuntu, Sans-Serif; fill: #2f80ed; }
    .stat { font: 600 14px 'Segoe UI', Ubuntu, Sans-Serif; fill: #434d58; }
    .statlabel { font: 400 12px 'Segoe UI', Ubuntu, Sans-Serif; fill: #586069; }
    .icon { fill: #586069; }
    .card-bg { fill: #fffefe; }
    .card-border { stroke: #e4e2e2; }

    @media (prefers-color-scheme: dark) {
      .stat { fill: #e1e4e8; }
      .statlabel { fill: #8b949e; }
      .icon { fill: #8b949e; }
      .card-bg { fill: #0d1117; }
      .card-border { stroke: #30363d; }
    }
  </style>

  <rect class="card-bg" x="0.5" y="0.5" rx="4.5" height="99%%" width="599" class="card-border" stroke-width="1" stroke-opacity="1"/>

  <g transform="translate(25, 30)">
    <!-- Total Stars -->
    <g transform="translate(0, 0)">
      <svg y="-2" viewBox="0 0 16 16" version="1.1" width="16" height="16">
        <path fill="#FFD700" fill-rule="evenodd" d="M8 .25a.75.75 0 01.673.418l1.882 3.815 4.21.612a.75.75 0 01.416 1.279l-3.046 2.97.719 4.192a.75.75 0 01-1.088.791L8 12.347l-3.766 1.98a.75.75 0 01-1.088-.79l.72-4.194L.818 6.374a.75.75 0 01.416-1.28l4.21-.611L7.327.668A.75.75 0 018 .25z"/>
      </svg>
      <text class="stat" x="25" y="12.5">Total Stars:</text>
      <text class="stat" x="135" y="12.5" font-weight="bold">%d</text>
    </g>

    <!-- Total Commits -->
    <g transform="translate(0, 35)">
      <svg y="-2" viewBox="0 0 16 16" version="1.1" width="16" height="16">
        <path class="icon" fill-rule="evenodd" d="M1.643 3.143L.427 1.927A.25.25 0 000 2.104V5.75c0 .138.112.25.25.25h3.646a.25.25 0 00.177-.427L2.715 4.215a6.5 6.5 0 11-1.18 4.458.75.75 0 10-1.493.154 8.001 8.001 0 101.6-5.684zM7.75 4a.75.75 0 01.75.75v2.992l2.028.812a.75.75 0 01-.557 1.392l-2.5-1A.75.75 0 017 8.25v-3.5A.75.75 0 017.75 4z"/>
      </svg>
      <text class="stat" x="25" y="12.5">Total Commits (2025):</text>
      <text class="stat" x="205" y="12.5" font-weight="bold">%d</text>
    </g>

    <!-- Total PRs -->
    <g transform="translate(0, 70)">
      <svg y="-2" viewBox="0 0 16 16" version="1.1" width="16" height="16">
        <path class="icon" fill-rule="evenodd" d="M7.177 3.073L9.573.677A.25.25 0 0110 .854v4.792a.25.25 0 01-.427.177L7.177 3.427a.25.25 0 010-.354zM3.75 2.5a.75.75 0 100 1.5.75.75 0 000-1.5zm-2.25.75a2.25 2.25 0 113 2.122v5.256a2.251 2.251 0 11-1.5 0V5.372A2.25 2.25 0 011.5 3.25zM11 2.5h-1V4h1a1 1 0 011 1v5.628a2.251 2.251 0 101.5 0V5A2.5 2.5 0 0011 2.5zm1 10.25a.75.75 0 111.5 0 .75.75 0 01-1.5 0zM3.75 12a.75.75 0 100 1.5.75.75 0 000-1.5z"/>
      </svg>
      <text class="stat" x="25" y="12.5">Total PRs:</text>
      <text class="stat" x="110" y="12.5" font-weight="bold">%d</text>
    </g>

    <!-- Total Issues -->
    <g transform="translate(300, 0)">
      <svg y="-2" viewBox="0 0 16 16" version="1.1" width="16" height="16">
        <path class="icon" fill-rule="evenodd" d="M8 1.5a6.5 6.5 0 100 13 6.5 6.5 0 000-13zM0 8a8 8 0 1116 0A8 8 0 010 8zm9 3a1 1 0 11-2 0 1 1 0 012 0zm-.25-6.25a.75.75 0 00-1.5 0v3.5a.75.75 0 001.5 0v-3.5z"/>
      </svg>
      <text class="stat" x="25" y="12.5">Total Issues:</text>
      <text class="stat" x="125" y="12.5" font-weight="bold">%d</text>
    </g>

    <!-- Contributed To -->
    <g transform="translate(300, 35)">
      <svg y="-2" viewBox="0 0 16 16" version="1.1" width="16" height="16">
        <path class="icon" fill-rule="evenodd" d="M2 2.5A2.5 2.5 0 014.5 0h8.75a.75.75 0 01.75.75v12.5a.75.75 0 01-.75.75h-2.5a.75.75 0 110-1.5h1.75v-2h-8a1 1 0 00-.714 1.7.75.75 0 01-1.072 1.05A2.495 2.495 0 012 11.5v-9zm10.5-1V9h-8c-.356 0-.694.074-1 .208V2.5a1 1 0 011-1h8zM5 12.25v3.25a.25.25 0 00.4.2l1.45-1.087a.25.25 0 01.3 0L8.6 15.7a.25.25 0 00.4-.2v-3.25a.25.25 0 00-.25-.25h-3.5a.25.25 0 00-.25.25z"/>
      </svg>
      <text class="stat" x="25" y="12.5">Contributed to:</text>
      <text class="stat" x="150" y="12.5" font-weight="bold">%d</text>
    </g>

    <!-- Total Contributions -->
    <g transform="translate(300, 70)">
      <svg y="-2" viewBox="0 0 16 16" version="1.1" width="16" height="16">
        <path class="icon" fill-rule="evenodd" d="M1.75 0A1.75 1.75 0 000 1.75v12.5C0 15.216.784 16 1.75 16h12.5A1.75 1.75 0 0016 14.25V1.75A1.75 1.75 0 0014.25 0H1.75zM1.5 1.75a.25.25 0 01.25-.25h12.5a.25.25 0 01.25.25v12.5a.25.25 0 01-.25.25H1.75a.25.25 0 01-.25-.25V1.75zM11.75 3a.75.75 0 00-.75.75v7.5a.75.75 0 001.5 0v-7.5a.75.75 0 00-.75-.75zm-8.25.75a.75.75 0 011.5 0v5.5a.75.75 0 01-1.5 0v-5.5zM8 3a.75.75 0 00-.75.75v3.5a.75.75 0 001.5 0v-3.5A.75.75 0 008 3z"/>
      </svg>
      <text class="stat" x="25" y="12.5">Contributions (2025):</text>
      <text class="stat" x="185" y="12.5" font-weight="bold">%d</text>
    </g>
  </g>
</svg>`,
		stats.TotalStars,
		stats.TotalCommits,
		stats.TotalPRs,
		stats.TotalIssues,
		stats.ContributedTo,
		stats.TotalContributions,
	)
}
