# Hi, I'm Rahul Gangwar 👋

I work on building, testing, and automating components of the Kubernetes and OpenShift control plane — with strong focus on API machinery, operators, e2e testing, and cluster reliability.

<img src="https://img.shields.io/badge/Cloud%20Native%20Engineer-0078D4?style=for-the-badge&logo=kubernetes&logoColor=white" alt="Cloud Native Engineer"/> <img src="https://img.shields.io/badge/OpenShift%20%26%20Kubernetes-EE0000?style=for-the-badge&logo=redhat&logoColor=white" alt="OpenShift & Kubernetes"/>

---

## 📈 GitHub Stats

![GitHub Stats](https://raw.githubusercontent.com/gangwgr/gangwgr/output/stats-card.svg)

---

## 🐍 Contribution Snake

<div align="center">

![Snake animation](https://raw.githubusercontent.com/gangwgr/gangwgr/output/github-contribution-grid-snake-dark.svg)

</div>

---

## 🛠️ About This Repository

This repository contains a custom GitHub stats card generator built in Go. The stats card is automatically generated daily via GitHub Actions and displays various GitHub contribution metrics.

### Features

- **Automatic Updates**: Stats card regenerates daily via GitHub Actions
- **Retry Logic**: Exponential backoff for resilient API calls
- **Rate Limit Handling**: Gracefully handles GitHub API rate limits
- **Dark Mode Support**: Automatically adapts to user's color scheme preference
- **Configurable**: Environment variables for customization
- **Accessibility**: SVG includes proper ARIA labels and semantic markup

---

## 🚀 Quick Start

### Prerequisites

- Go 1.23 or higher
- GitHub Personal Access Token with `read:user` and `repo` scopes

### Installation

1. Clone this repository:
```bash
git clone https://github.com/gangwgr/gangwgr.git
cd gangwgr
```

2. Install dependencies:
```bash
go mod download
```

3. Set up your GitHub token:
```bash
export GITHUB_TOKEN="your_github_token_here"
```

### Usage

#### Generate Stats Card

```bash
# Using Go run
make run

# Or build and run
make build
./bin/generate-stats-card
```

#### Configuration Options

Configure the generator using environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `GITHUB_TOKEN` | GitHub Personal Access Token (required) | - |
| `GITHUB_USERNAME` | GitHub username to generate stats for | `gangwgr` |
| `OUTPUT_DIR` | Directory to save the generated SVG | `output` |
| `OUTPUT_FILE` | Filename for the SVG | `stats-card.svg` |
| `VERBOSE` | Enable verbose logging | `false` |

Example with custom configuration:
```bash
export GITHUB_TOKEN="ghp_xxxxx"
export GITHUB_USERNAME="yourusername"
export OUTPUT_DIR="custom-output"
export VERBOSE=true
make run
```

---

## 📦 Available Make Commands

```bash
make help           # Show available commands
make build          # Build the binary
make run            # Generate stats card
make clean          # Remove generated files
make fmt            # Format Go code
make lint           # Run linters
make test           # Run tests
make check          # Run formatting and linting
make generate       # Clean and regenerate stats card
make install-tools  # Install development tools
```

---

## 🔄 GitHub Actions Automation

The stats card is automatically regenerated daily at 00:00 UTC using GitHub Actions. The workflow:

1. Fetches the latest contribution data from GitHub API
2. Generates a new SVG stats card
3. Commits and pushes the updated card to the `output` branch

To set up automation for your own fork:

1. Fork this repository
2. Go to Settings → Secrets and variables → Actions
3. Add a new secret named `STATS_TOKEN` with your GitHub Personal Access Token
4. The workflow will run automatically daily

---

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
make test

# Run tests with verbose output
go test -v ./...

# Run tests with coverage
go test -v -race -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

---

## 📊 Stats Card Metrics

The generated card displays:

- **Total Stars**: Stars across all your repositories
- **Total Commits**: Commits made in 2025
- **Total PRs**: Pull requests opened in 2025
- **Total Issues**: Issues opened in 2025
- **Contributed To**: Number of repositories you've contributed to
- **Total Contributions**: Total contributions in 2025

---

## 🎨 Customization

### Styling the SVG

Edit the SVG styles in `generate-stats-card.go` in the `generateStatsCardSVG` function. The SVG uses CSS for styling and includes both light and dark mode themes.

### Adding New Metrics

To add new metrics:

1. Update the GraphQL query in `fetchStats()` function
2. Add fields to the `StatsCard` struct
3. Update the `GitHubResponse` struct to match the API response
4. Modify `generateStatsCardSVG()` to display the new metrics

---

## 🤝 Contributing

Contributions are welcome! Feel free to:

- Report bugs
- Suggest new features
- Submit pull requests
- Improve documentation

---

## 📝 License

This project is open source and available under the MIT License.

---

## 🔗 Links

- [My GitHub](https://github.com/gangwgr)
- [Report Issues](https://github.com/gangwgr/gangwgr/issues)

---

<div align="center">

**Built with ❤️ using Go and GitHub Actions**

</div>
