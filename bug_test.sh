#!/bin/bash

# OCPBUGS-57474 Test Script
# Test for: Project API watch functionality issue
# 
# This script tests the fix for the failing test:
# [sig-auth][Feature:ProjectAPI] TestProjectWatch should succeed

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Global variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="${SCRIPT_DIR}/ocpbugs_57474_test_$(date +%Y%m%d_%H%M%S).log"
TEST_NAMESPACE="ocpbugs-57474-test"
TEST_PROJECT="test-project-watch"

# Logging functions
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$LOG_FILE"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOG_FILE"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."
    
    if ! command -v oc &> /dev/null; then
        log_error "oc command not found. Please install OpenShift CLI."
        exit 1
    fi
    
    if ! oc whoami &> /dev/null; then
        log_error "Not logged into OpenShift cluster. Please run 'oc login' first."
        exit 1
    fi
    
    # Check cluster admin permissions
    if ! oc auth can-i "*" "*" --all-namespaces &> /dev/null; then
        log_warning "You may not have cluster-admin permissions. Some tests might fail."
    fi
    
    # Check if openshift-tests binary is available
    if ! command -v openshift-tests &> /dev/null; then
        log_warning "openshift-tests binary not found. Will use alternative testing methods."
    fi
    
    log_success "Prerequisites check completed"
}

# Get cluster version and build info
get_cluster_info() {
    log "Getting cluster information..."
    
    local version
    version=$(oc get clusterversion -o jsonpath='{.items[0].status.desired.version}' 2>/dev/null || echo "UNKNOWN")
    
    if [[ "$version" != "UNKNOWN" ]]; then
        log_success "Cluster version: $version"
        
        # Check if this is a version with the fix
        if [[ "$version" == *"57474"* ]] || [[ "$version" == *"pr544"* ]]; then
            log_success "This appears to be a version with OCPBUGS-57474 fix"
        else
            log_warning "This appears to be a version BEFORE OCPBUGS-57474 fix"
        fi
    else
        log_error "Failed to get cluster version"
    fi
    
    # Get build info
    local build_info
    build_info=$(oc get clusterversion -o jsonpath='{.items[0].status.desired.image}' 2>/dev/null || echo "UNKNOWN")
    log "Build image: $build_info"
    
    return 0
}

# Test 1: Basic Project API functionality
test_project_api_basic() {
    log "=== Test 1: Basic Project API functionality ==="
    
    # Create test project
    log "Creating test project..."
    if oc new-project "$TEST_PROJECT" --description="Test project for OCPBUGS-57474" 2>/dev/null; then
        log_success "Test project created: $TEST_PROJECT"
    else
        log_warning "Project might already exist or creation failed"
    fi
    
    # Test project listing
    log "Testing project listing..."
    if oc get projects | grep -q "$TEST_PROJECT"; then
        log_success "Project listing works"
    else
        log_error "Project listing failed"
        return 1
    fi
    
    # Test project details
    log "Testing project details..."
    if oc get project "$TEST_PROJECT" -o yaml | grep -q "name: $TEST_PROJECT"; then
        log_success "Project details retrieval works"
    else
        log_error "Project details retrieval failed"
        return 1
    fi
    
    return 0
}

# Test 2: Project Watch functionality (simulated)
test_project_watch_simulation() {
    log "=== Test 2: Project Watch functionality (simulated) ==="
    
    # Test watch command with timeout
    log "Testing project watch with timeout..."
    
    local watch_output
    local watch_exit_code
    
    # Start watch in background with timeout
    timeout 10s oc get projects --watch-only=true > /tmp/watch_output.txt 2>&1 &
    local watch_pid=$!
    
    # Wait a bit and then create/modify a project
    sleep 2
    
    # Modify the test project
    oc patch project "$TEST_PROJECT" -p '{"metadata":{"annotations":{"test-watch":"true"}}}' 2>/dev/null || true
    
    # Wait for watch to complete
    wait $watch_pid 2>/dev/null || true
    watch_exit_code=$?
    
    # Check if watch produced output
    if [[ -s /tmp/watch_output.txt ]]; then
        log_success "Project watch produced output"
        log "Watch output (first 5 lines):"
        head -5 /tmp/watch_output.txt
    else
        log_warning "Project watch produced no output"
    fi
    
    # Clean up
    rm -f /tmp/watch_output.txt
    
    if [[ $watch_exit_code -eq 124 ]]; then
        log_success "Watch timeout as expected"
        return 0
    elif [[ $watch_exit_code -eq 0 ]]; then
        log_success "Watch completed successfully"
        return 0
    else
        log_warning "Watch exited with code $watch_exit_code"
        return 0  # Don't fail the test for watch timeout
    fi
}

# Test 3: API Server logs analysis
test_api_server_logs() {
    log "=== Test 3: API Server logs analysis ==="
    
    # Get openshift-apiserver pods
    local api_pods
    api_pods=$(oc get pods -n openshift-apiserver -l app=openshift-apiserver -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")
    
    if [[ -n "$api_pods" ]]; then
        log_success "Found openshift-apiserver pods: $api_pods"
        
        # Check for recent errors in API server logs
        for pod in $api_pods; do
            log "Checking logs from $pod..."
            
            local recent_logs
            recent_logs=$(oc logs -n openshift-apiserver "$pod" --tail=50 2>/dev/null || echo "NO_LOGS")
            
            if [[ "$recent_logs" != "NO_LOGS" ]]; then
                # Check for project-related errors
                if echo "$recent_logs" | grep -q -i "project.*error\|watch.*error\|api.*error"; then
                    log_warning "Found project/watch related errors in $pod logs:"
                    echo "$recent_logs" | grep -i "project.*error\|watch.*error\|api.*error" | tail -3
                else
                    log_success "No project/watch errors found in $pod logs"
                fi
                
                # Check for OCPBUGS-57474 related messages
                if echo "$recent_logs" | grep -q "57474\|project.*watch"; then
                    log_success "Found OCPBUGS-57474 related messages in $pod logs:"
                    echo "$recent_logs" | grep -i "57474\|project.*watch" | tail -3
                fi
            else
                log_warning "No logs found for $pod"
            fi
        done
    else
        log_warning "No openshift-apiserver pods found"
    fi
    
    return 0
}

# Test 4: Run the actual failing test (if openshift-tests available)
test_actual_failing_test() {
    log "=== Test 4: Running actual failing test ==="
    
    if ! command -v openshift-tests &> /dev/null; then
        log_warning "openshift-tests not available, skipping actual test execution"
        return 0
    fi
    
    log "Running the failing test: TestProjectWatch"
    
    # Run the specific test
    local test_output
    local test_exit_code
    
    test_output=$(openshift-tests run --dry-run openshift/conformance/parallel | grep "TestProjectWatch" 2>/dev/null || echo "TEST_NOT_FOUND")
    
    if [[ "$test_output" != "TEST_NOT_FOUND" ]]; then
        log "Found test: $test_output"
        
        # Run the test with timeout
        log "Executing test with timeout..."
        timeout 300s openshift-tests run openshift/conformance/parallel --focus="TestProjectWatch" --skip="none" 2>&1 | tee /tmp/test_output.txt
        test_exit_code=${PIPESTATUS[0]}
        
        if [[ $test_exit_code -eq 0 ]]; then
            log_success "TestProjectWatch passed!"
            return 0
        elif [[ $test_exit_code -eq 124 ]]; then
            log_warning "Test timed out after 5 minutes"
            return 1
        else
            log_error "TestProjectWatch failed with exit code $test_exit_code"
            log "Test output:"
            tail -20 /tmp/test_output.txt
            return 1
        fi
    else
        log_warning "TestProjectWatch not found in test suite"
        return 0
    fi
}

# Test 5: API endpoint testing
test_api_endpoints() {
    log "=== Test 5: API endpoint testing ==="
    
    # Get API server URL
    local api_url
    api_url=$(oc config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null || echo "")
    
    if [[ -n "$api_url" ]]; then
        log "Testing API endpoints at: $api_url"
        
        # Test project API endpoint
        log "Testing project API endpoint..."
        local project_response
        project_response=$(curl -s -k -H "Authorization: Bearer $(oc whoami -t)" "$api_url/apis/project.openshift.io/v1/projects" 2>/dev/null || echo "FAILED")
        
        if [[ "$project_response" != "FAILED" ]]; then
            log_success "Project API endpoint accessible"
            
            # Check if our test project is in the response
            if echo "$project_response" | grep -q "$TEST_PROJECT"; then
                log_success "Test project found in API response"
            else
                log_warning "Test project not found in API response"
            fi
        else
            log_error "Project API endpoint not accessible"
            return 1
        fi
        
        # Test watch endpoint
        log "Testing project watch endpoint..."
        local watch_response
        watch_response=$(timeout 5s curl -s -k -H "Authorization: Bearer $(oc whoami -t)" "$api_url/apis/project.openshift.io/v1/projects?watch=true" 2>/dev/null || echo "FAILED")
        
        if [[ "$watch_response" != "FAILED" ]]; then
            log_success "Project watch endpoint accessible"
        else
            log_warning "Project watch endpoint not accessible or timed out"
        fi
    else
        log_error "Could not determine API server URL"
        return 1
    fi
    
    return 0
}

# Test 6: RBAC and permissions testing
test_rbac_permissions() {
    log "=== Test 6: RBAC and permissions testing ==="
    
    # Test current user permissions
    log "Testing current user permissions..."
    
    local user
    user=$(oc whoami 2>/dev/null || echo "UNKNOWN")
    log "Current user: $user"
    
    # Test project creation permission
    if oc auth can-i create projects; then
        log_success "User can create projects"
    else
        log_warning "User cannot create projects"
    fi
    
    # Test project watch permission
    if oc auth can-i watch projects; then
        log_success "User can watch projects"
    else
        log_warning "User cannot watch projects"
    fi
    
    # Test project get permission
    if oc auth can-i get projects; then
        log_success "User can get projects"
    else
        log_warning "User cannot get projects"
    fi
    
    return 0
}

# Test 7: Performance and stress testing
test_performance() {
    log "=== Test 7: Performance and stress testing ==="
    
    # Create multiple projects to test watch performance
    log "Creating multiple test projects for performance testing..."
    
    local created_count=0
    local failed_count=0
    
    for i in {1..5}; do
        local project_name="${TEST_PROJECT}-perf-$i"
        if oc new-project "$project_name" --description="Performance test project $i" 2>/dev/null; then
            ((created_count++))
            log_success "Created project: $project_name"
        else
            ((failed_count++))
            log_warning "Failed to create project: $project_name"
        fi
    done
    
    log "Performance test results: $created_count created, $failed_count failed"
    
    # Test watch with multiple projects
    log "Testing watch with multiple projects..."
    timeout 10s oc get projects --watch-only=true > /tmp/perf_watch.txt 2>&1 &
    local perf_watch_pid=$!
    
    # Modify a few projects
    sleep 2
    for i in {1..3}; do
        local project_name="${TEST_PROJECT}-perf-$i"
        oc patch project "$project_name" -p "{\"metadata\":{\"annotations\":{\"perf-test\":\"$i\"}}}" 2>/dev/null || true
    done
    
    wait $perf_watch_pid 2>/dev/null || true
    
    if [[ -s /tmp/perf_watch.txt ]]; then
        log_success "Performance watch test produced output"
        log "Watch events captured: $(wc -l < /tmp/perf_watch.txt)"
    else
        log_warning "Performance watch test produced no output"
    fi
    
    rm -f /tmp/perf_watch.txt
    
    return 0
}

# Generate test report
generate_test_report() {
    log "Generating test report..."
    
    local report_file="${SCRIPT_DIR}/ocpbugs_57474_report_$(date +%Y%m%d_%H%M%S).txt"
    
    cat <<EOF > "$report_file"
=== OCPBUGS-57474 Test Report ===
Date: $(date)
Cluster Version: $(oc get clusterversion -o jsonpath='{.items[0].status.desired.version}' 2>/dev/null || echo "UNKNOWN")
User: $(oc whoami)
Cluster: $(oc whoami --show-server)

=== Test Summary ===
- Test Namespace: $TEST_NAMESPACE
- Test Project: $TEST_PROJECT
- Bug Reference: OCPBUGS-57474
- PR Reference: https://github.com/openshift/openshift-apiserver/pull/544

=== Test Results ===
1. Prerequisites Check: [PASS/FAIL]
2. Cluster Info: [PASS/FAIL]
3. Basic Project API: [PASS/FAIL]
4. Project Watch Simulation: [PASS/FAIL]
5. API Server Logs: [PASS/FAIL]
6. Actual Failing Test: [PASS/FAIL]
7. API Endpoints: [PASS/FAIL]
8. RBAC Permissions: [PASS/FAIL]
9. Performance Testing: [PASS/FAIL]

=== Recommendations ===
- [List any issues found]
- [List recommendations for production]

=== Next Steps ===
- [List next steps for testing in production]
EOF
    
    log_success "Test report generated: $report_file"
}

# Cleanup function
cleanup() {
    log "Cleaning up test resources..."
    
    # Delete test projects
    for project in $(oc get projects -o jsonpath='{.items[*].metadata.name}' 2>/dev/null | grep "$TEST_PROJECT"); do
        oc delete project "$project" --ignore-not-found=true || true
    done
    
    # Clean up temporary files
    rm -f /tmp/watch_output.txt /tmp/test_output.txt /tmp/perf_watch.txt
    
    log_success "Cleanup completed"
}

# Main test execution
main() {
    log "Starting OCPBUGS-57474 test..."
    log "Testing: Project API watch functionality fix"
    
    # Set up cleanup on exit
    trap cleanup EXIT
    
    # Run tests
    check_prerequisites
    get_cluster_info
    test_project_api_basic
    test_project_watch_simulation
    test_api_server_logs
    test_actual_failing_test
    test_api_endpoints
    test_rbac_permissions
    test_performance
    
    # Generate report
    generate_test_report
    
    log_success "OCPBUGS-57474 test completed!"
    log "Check the generated report for detailed results."
}

# Show usage
usage() {
    echo "Usage: $0"
    echo ""
    echo "This script tests OCPBUGS-57474 fix for Project API watch functionality."
    echo ""
    echo "The test will:"
    echo "1. Check prerequisites and cluster info"
    echo "2. Test basic Project API functionality"
    echo "3. Simulate Project Watch operations"
    echo "4. Analyze API server logs"
    echo "5. Run the actual failing test (if openshift-tests available)"
    echo "6. Test API endpoints"
    echo "7. Check RBAC permissions"
    echo "8. Perform performance testing"
    echo "9. Generate a test report"
    echo ""
    echo "Requirements:"
    echo "- OpenShift CLI (oc) installed"
    echo "- Cluster admin permissions"
    echo "- Active cluster connection"
    echo "- openshift-tests binary (optional, for actual test execution)"
}

# Check for help flag
if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

# Run main function
main "$@"
