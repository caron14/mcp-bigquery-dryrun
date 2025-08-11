#!/bin/bash

# mcp-bigquery-dryrun Test Script
# 
# Usage:
#   ./test.sh              # Run all tests
#   ./test.sh --quick      # Run only unit tests (no credentials required)
#   ./test.sh --release    # Run full release test suite
#   ./test.sh --import     # Run only import tests
#   ./test.sh --coverage   # Run tests with coverage report

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}==>${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Header
echo "========================================="
echo "mcp-bigquery-dryrun Test Suite"
echo "========================================="
echo ""

# Check Python version
print_status "Checking Python version..."
python_version=$(python --version 2>&1 | cut -d' ' -f2)
required_version="3.9"
if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" = "$required_version" ]; then
    print_success "Python $python_version (>= $required_version)"
else
    print_error "Python $python_version is too old. Required: >= $required_version"
    exit 1
fi

# Check if pytest is installed
print_status "Checking pytest installation..."
if command_exists pytest; then
    print_success "pytest is installed"
else
    print_error "pytest is not installed. Run: pip install -e '.[dev]'"
    exit 1
fi

# Check for BigQuery credentials
has_credentials=false
if [ -n "$GOOGLE_APPLICATION_CREDENTIALS" ] || \
   [ -f "$HOME/.config/gcloud/application_default_credentials.json" ] || \
   [ -n "$GOOGLE_CLOUD_PROJECT" ]; then
    has_credentials=true
    print_success "BigQuery credentials found"
else
    print_warning "No BigQuery credentials found. Integration tests will be skipped."
fi

# Parse command line arguments
test_mode="default"
if [ $# -eq 1 ]; then
    case $1 in
        --quick)
            test_mode="quick"
            ;;
        --release)
            test_mode="release"
            ;;
        --import)
            test_mode="import"
            ;;
        --coverage)
            test_mode="coverage"
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --quick     Run only unit tests (no credentials required)"
            echo "  --release   Run full release test suite"
            echo "  --import    Run only import tests"
            echo "  --coverage  Run tests with coverage report"
            echo "  --help      Show this help message"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Run '$0 --help' for usage information"
            exit 1
            ;;
    esac
fi

echo ""
print_status "Running tests in '$test_mode' mode..."
echo ""

# Run tests based on mode
case $test_mode in
    quick)
        print_status "Running unit tests only (no credentials required)..."
        pytest tests/test_min.py::TestWithoutCredentials tests/test_imports.py -v --tb=short
        ;;
    
    release)
        print_status "Running full release test suite..."
        if [ "$has_credentials" = true ]; then
            python run_release_tests.py
        else
            python run_release_tests.py --skip-integration
        fi
        ;;
    
    import)
        print_status "Running import tests..."
        pytest tests/test_imports.py -v --tb=short
        ;;
    
    coverage)
        print_status "Running tests with coverage report..."
        if command_exists coverage; then
            coverage run -m pytest tests/ -v
            coverage report -m
            coverage html
            print_success "Coverage report generated in htmlcov/index.html"
        else
            print_error "coverage is not installed. Run: pip install coverage"
            exit 1
        fi
        ;;
    
    default)
        # Run standard test suite
        if [ "$has_credentials" = true ]; then
            print_status "Running full test suite with integration tests..."
            pytest tests/ -v --tb=short
        else
            print_status "Running test suite without integration tests..."
            pytest tests/ -v --tb=short -m "not requires_credentials"
        fi
        ;;
esac

# Check exit code
if [ $? -eq 0 ]; then
    echo ""
    echo "========================================="
    print_success "All tests passed!"
    echo "========================================="
else
    echo ""
    echo "========================================="
    print_error "Some tests failed"
    echo "========================================="
    exit 1
fi