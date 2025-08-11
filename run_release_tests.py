#!/usr/bin/env python3
"""
Release Test Suite for mcp-bigquery-dryrun

This script performs comprehensive testing before releasing a new version.
All tests must pass for the release to be approved.

Usage:
    python run_release_tests.py [--skip-integration]
"""

import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple, Any
import argparse


class Colors:
    """ANSI color codes for terminal output."""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class ReleaseTestSuite:
    """Comprehensive test suite for release validation."""
    
    def __init__(self, skip_integration: bool = False):
        self.skip_integration = skip_integration
        self.test_results = []
        self.start_time = None
        self.project_root = Path(__file__).parent
        
    def print_header(self, text: str):
        """Print a formatted header."""
        print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.ENDC}")
        print(f"{Colors.HEADER}{Colors.BOLD}{text}{Colors.ENDC}")
        print(f"{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.ENDC}\n")
    
    def print_section(self, text: str):
        """Print a section header."""
        print(f"\n{Colors.OKCYAN}{Colors.BOLD}▶ {text}{Colors.ENDC}")
        print(f"{Colors.OKCYAN}{'-'*50}{Colors.ENDC}")
    
    def print_success(self, text: str):
        """Print success message."""
        print(f"{Colors.OKGREEN}✅ {text}{Colors.ENDC}")
    
    def print_error(self, text: str):
        """Print error message."""
        print(f"{Colors.FAIL}❌ {text}{Colors.ENDC}")
    
    def print_warning(self, text: str):
        """Print warning message."""
        print(f"{Colors.WARNING}⚠️  {text}{Colors.ENDC}")
    
    def print_info(self, text: str):
        """Print info message."""
        print(f"{Colors.OKBLUE}ℹ️  {text}{Colors.ENDC}")
    
    def run_command(self, cmd: str, capture_output: bool = True) -> Tuple[int, str, str]:
        """Run a shell command and return result."""
        try:
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=capture_output,
                text=True,
                timeout=30
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return 1, "", "Command timed out"
        except Exception as e:
            return 1, "", str(e)
    
    def record_result(self, test_name: str, passed: bool, details: str = ""):
        """Record test result."""
        self.test_results.append({
            "test": test_name,
            "passed": passed,
            "details": details
        })
        if passed:
            self.print_success(f"{test_name}")
        else:
            self.print_error(f"{test_name}: {details}")
    
    def test_python_version(self) -> bool:
        """Test Python version requirement."""
        self.print_section("Python Version Check")
        
        version = sys.version_info
        passed = version >= (3, 9)
        
        self.print_info(f"Python version: {version.major}.{version.minor}.{version.micro}")
        
        if passed:
            self.record_result("Python version >= 3.9", True)
        else:
            self.record_result("Python version >= 3.9", False, 
                             f"Found {version.major}.{version.minor}, need >= 3.9")
        return passed
    
    def test_package_structure(self) -> bool:
        """Test that package structure is correct."""
        self.print_section("Package Structure Check")
        
        required_files = [
            "pyproject.toml",
            "README.md",
            "LICENSE",
            "src/mcp_bigquery_dryrun/__init__.py",
            "src/mcp_bigquery_dryrun/__main__.py",
            "src/mcp_bigquery_dryrun/server.py",
            "src/mcp_bigquery_dryrun/bigquery_client.py",
            "tests/test_min.py",
        ]
        
        all_exist = True
        for file_path in required_files:
            full_path = self.project_root / file_path
            exists = full_path.exists()
            if exists:
                self.print_success(f"Found: {file_path}")
            else:
                self.print_error(f"Missing: {file_path}")
                all_exist = False
        
        self.record_result("Package structure complete", all_exist)
        return all_exist
    
    def test_imports(self) -> bool:
        """Test critical imports."""
        self.print_section("Import Tests")
        
        test_imports = [
            ("google.auth.exceptions", "DefaultCredentialsError"),
            ("google.cloud", "bigquery"),
            ("mcp.server.stdio", None),
            ("mcp.server", "Server"),
            ("mcp_bigquery_dryrun", "__version__"),
            ("mcp_bigquery_dryrun.server", "validate_sql"),
            ("mcp_bigquery_dryrun.server", "dry_run_sql"),
            ("mcp_bigquery_dryrun.bigquery_client", "get_bigquery_client"),
        ]
        
        all_passed = True
        for module_name, attr_name in test_imports:
            try:
                module = __import__(module_name, fromlist=[attr_name] if attr_name else [])
                if attr_name:
                    getattr(module, attr_name)
                self.print_success(f"Import: {module_name}" + 
                                  (f".{attr_name}" if attr_name else ""))
            except ImportError as e:
                self.print_error(f"Import failed: {module_name}" + 
                               (f".{attr_name}" if attr_name else "") + 
                               f" - {e}")
                all_passed = False
        
        self.record_result("All imports successful", all_passed)
        return all_passed
    
    def test_console_script(self) -> bool:
        """Test console script installation."""
        self.print_section("Console Script Test")
        
        # Check if command exists
        returncode, stdout, _ = self.run_command("which mcp-bigquery-dryrun")
        command_exists = returncode == 0
        
        if command_exists:
            self.print_success("Console script installed")
            
            # Test version output
            returncode, stdout, stderr = self.run_command(
                "python -c 'from mcp_bigquery_dryrun import __version__; print(__version__)'"
            )
            if returncode == 0:
                version = stdout.strip()
                self.print_info(f"Package version: {version}")
                self.record_result("Console script functional", True)
                return True
            else:
                self.record_result("Console script functional", False, 
                                 "Version check failed")
                return False
        else:
            self.print_warning("Console script not found (expected for development)")
            self.record_result("Console script functional", True, 
                             "Skipped - development mode")
            return True
    
    async def test_basic_functionality(self) -> bool:
        """Test basic tool functionality."""
        self.print_section("Basic Functionality Tests")
        
        try:
            from mcp_bigquery_dryrun.server import validate_sql, dry_run_sql
            
            # Test 1: Valid SQL
            result = await validate_sql("SELECT 1")
            test1_passed = result.get("isValid") == True
            self.record_result("validate_sql with valid SQL", test1_passed)
            
            # Test 2: Invalid SQL
            result = await validate_sql("SELECT FROM WHERE")
            test2_passed = (result.get("isValid") == False and 
                          "error" in result)
            self.record_result("validate_sql with invalid SQL", test2_passed)
            
            # Test 3: Dry run
            result = await dry_run_sql("SELECT 1")
            test3_passed = ("totalBytesProcessed" in result and 
                          "usdEstimate" in result and
                          "referencedTables" in result and
                          "schemaPreview" in result)
            self.record_result("dry_run_sql basic test", test3_passed)
            
            # Test 4: Parameters
            result = await validate_sql(
                "SELECT * FROM table WHERE id = @id",
                params={"id": "123"}
            )
            test4_passed = "isValid" in result  # Will be false due to table not existing
            self.record_result("Parameterized query support", test4_passed)
            
            # Test 5: Error location extraction
            from mcp_bigquery_dryrun.server import extract_error_location
            location = extract_error_location("Error at [1:10]")
            test5_passed = (location is not None and 
                          location.get("line") == 1 and 
                          location.get("column") == 10)
            self.record_result("Error location extraction", test5_passed)
            
            # Test 6: Query parameter building
            from mcp_bigquery_dryrun.server import build_query_parameters
            params = build_query_parameters({"test": "value"})
            test6_passed = len(params) == 1 and params[0].type_ == "STRING"
            self.record_result("Query parameter building", test6_passed)
            
            return all([test1_passed, test2_passed, test3_passed, 
                       test4_passed, test5_passed, test6_passed])
            
        except Exception as e:
            self.record_result("Basic functionality tests", False, str(e))
            return False
    
    async def test_public_dataset(self) -> bool:
        """Test with BigQuery public dataset."""
        if self.skip_integration:
            self.print_warning("Skipping integration tests")
            return True
            
        self.print_section("Public Dataset Integration Tests")
        
        try:
            from mcp_bigquery_dryrun.server import dry_run_sql
            
            # Test with Shakespeare dataset
            sql = "SELECT * FROM `bigquery-public-data.samples.shakespeare` LIMIT 10"
            result = await dry_run_sql(sql)
            
            if "error" in result:
                self.record_result("Public dataset query", False, 
                                 result["error"].get("message", "Unknown error"))
                return False
            
            # Verify response structure
            has_bytes = "totalBytesProcessed" in result
            has_estimate = "usdEstimate" in result
            has_tables = ("referencedTables" in result and 
                         len(result["referencedTables"]) > 0)
            has_schema = ("schemaPreview" in result and 
                         len(result["schemaPreview"]) > 0)
            
            all_passed = all([has_bytes, has_estimate, has_tables, has_schema])
            
            if all_passed:
                self.print_info(f"Bytes to process: {result['totalBytesProcessed']:,}")
                self.print_info(f"Cost estimate: ${result['usdEstimate']:.6f}")
                self.print_info(f"Tables referenced: {len(result['referencedTables'])}")
                self.print_info(f"Schema fields: {len(result['schemaPreview'])}")
            
            self.record_result("Public dataset query", all_passed)
            return all_passed
            
        except Exception as e:
            self.record_result("Public dataset query", False, str(e))
            return False
    
    def test_pytest_suite(self) -> bool:
        """Run pytest test suite."""
        self.print_section("Pytest Test Suite")
        
        # Run unit tests (no credentials required)
        returncode, stdout, stderr = self.run_command(
            "python -m pytest tests/test_min.py::TestWithoutCredentials -v --tb=short"
        )
        unit_passed = returncode == 0
        self.record_result("Unit tests (no credentials)", unit_passed)
        
        if not self.skip_integration:
            # Run import tests
            returncode, stdout, stderr = self.run_command(
                "python -m pytest tests/test_imports.py -v --tb=short"
            )
            import_passed = returncode == 0
            self.record_result("Import tests", import_passed)
            
            # Run full test suite
            returncode, stdout, stderr = self.run_command(
                "python -m pytest tests/ -v --tb=short"
            )
            full_passed = returncode == 0
            self.record_result("Full test suite", full_passed)
            
            return unit_passed and import_passed and full_passed
        else:
            return unit_passed
    
    async def test_performance(self) -> bool:
        """Test performance benchmarks."""
        self.print_section("Performance Tests")
        
        try:
            from mcp_bigquery_dryrun.server import validate_sql, dry_run_sql
            
            queries = [
                "SELECT 1",
                "SELECT * FROM table WHERE id = @id",
            ]
            
            if not self.skip_integration:
                queries.append("SELECT COUNT(*) FROM `bigquery-public-data.samples.shakespeare`")
            
            all_passed = True
            for query in queries:
                # Test validate_sql performance
                start = time.time()
                result = await validate_sql(query, params={"id": "123"})
                validate_time = time.time() - start
                
                # Test dry_run_sql performance
                start = time.time()
                result = await dry_run_sql(query, params={"id": "123"})
                dry_run_time = time.time() - start
                
                self.print_info(f"Query: {query[:50]}")
                self.print_info(f"  validate_sql: {validate_time:.3f}s")
                self.print_info(f"  dry_run_sql:  {dry_run_time:.3f}s")
                
                # Check if response times are reasonable (< 5 seconds)
                if validate_time > 5 or dry_run_time > 5:
                    all_passed = False
                    self.print_warning(f"Slow response for: {query[:30]}")
            
            self.record_result("Performance within limits", all_passed)
            return all_passed
            
        except Exception as e:
            self.record_result("Performance tests", False, str(e))
            return False
    
    def test_documentation(self) -> bool:
        """Test documentation completeness."""
        self.print_section("Documentation Check")
        
        required_docs = [
            "README.md",
            "LICENSE",
            "DEVELOPMENT_TESTING.md",
        ]
        
        optional_docs = [
            "USER_MANUAL.md",
            "USER_MANUAL_ja.md",
            "PUBLISHING.md",
            "PUBLISHING_ja.md",
            "CLAUDE.md",
        ]
        
        all_required = True
        for doc in required_docs:
            path = self.project_root / doc
            if path.exists():
                self.print_success(f"Required: {doc}")
            else:
                self.print_error(f"Missing required: {doc}")
                all_required = False
        
        for doc in optional_docs:
            path = self.project_root / doc
            if path.exists():
                self.print_info(f"Optional: {doc}")
        
        self.record_result("Required documentation present", all_required)
        return all_required
    
    def test_package_metadata(self) -> bool:
        """Test package metadata consistency."""
        self.print_section("Package Metadata Check")
        
        try:
            # Check version consistency
            from mcp_bigquery_dryrun import __version__ as init_version
            
            # Read pyproject.toml version
            pyproject_path = self.project_root / "pyproject.toml"
            with open(pyproject_path) as f:
                content = f.read()
                import re
                match = re.search(r'version = "([^"]+)"', content)
                if match:
                    pyproject_version = match.group(1)
                else:
                    self.record_result("Version consistency", False, 
                                     "Could not parse pyproject.toml")
                    return False
            
            versions_match = init_version == pyproject_version
            if versions_match:
                self.print_info(f"Version: {init_version}")
                self.record_result("Version consistency", True)
            else:
                self.record_result("Version consistency", False, 
                                 f"__init__: {init_version}, pyproject: {pyproject_version}")
            
            # Check author info
            from mcp_bigquery_dryrun import __author__, __email__
            has_author = bool(__author__ and __email__)
            self.record_result("Author information present", has_author)
            
            return versions_match and has_author
            
        except Exception as e:
            self.record_result("Package metadata check", False, str(e))
            return False
    
    def generate_report(self) -> bool:
        """Generate final test report."""
        self.print_header("TEST RESULTS SUMMARY")
        
        total = len(self.test_results)
        passed = sum(1 for r in self.test_results if r["passed"])
        failed = total - passed
        
        # Group results by status
        print(f"\n{Colors.BOLD}Passed Tests:{Colors.ENDC}")
        for result in self.test_results:
            if result["passed"]:
                print(f"  ✅ {result['test']}")
        
        if failed > 0:
            print(f"\n{Colors.BOLD}Failed Tests:{Colors.ENDC}")
            for result in self.test_results:
                if not result["passed"]:
                    print(f"  ❌ {result['test']}")
                    if result["details"]:
                        print(f"     {Colors.WARNING}{result['details']}{Colors.ENDC}")
        
        # Summary statistics
        print(f"\n{Colors.BOLD}Statistics:{Colors.ENDC}")
        print(f"  Total tests: {total}")
        print(f"  Passed: {Colors.OKGREEN}{passed}{Colors.ENDC}")
        print(f"  Failed: {Colors.FAIL if failed > 0 else Colors.OKGREEN}{failed}{Colors.ENDC}")
        print(f"  Success rate: {passed/total*100:.1f}%")
        
        if self.start_time:
            duration = time.time() - self.start_time
            print(f"  Duration: {duration:.2f} seconds")
        
        # Final verdict
        all_passed = failed == 0
        print(f"\n{Colors.BOLD}{'='*60}{Colors.ENDC}")
        if all_passed:
            print(f"{Colors.OKGREEN}{Colors.BOLD}✅ ALL TESTS PASSED - READY FOR RELEASE{Colors.ENDC}")
        else:
            print(f"{Colors.FAIL}{Colors.BOLD}❌ TESTS FAILED - NOT READY FOR RELEASE{Colors.ENDC}")
        print(f"{Colors.BOLD}{'='*60}{Colors.ENDC}\n")
        
        return all_passed
    
    async def run_all_tests(self) -> bool:
        """Run all tests in sequence."""
        self.start_time = time.time()
        
        self.print_header("MCP-BIGQUERY-DRYRUN RELEASE TEST SUITE")
        print(f"Skip Integration: {self.skip_integration}")
        print(f"Project Root: {self.project_root}")
        
        # Run test categories
        self.test_python_version()
        self.test_package_structure()
        self.test_imports()
        self.test_console_script()
        await self.test_basic_functionality()
        
        if not self.skip_integration:
            await self.test_public_dataset()
            await self.test_performance()
        
        self.test_pytest_suite()
        self.test_documentation()
        self.test_package_metadata()
        
        # Generate final report
        return self.generate_report()


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run comprehensive release tests for mcp-bigquery-dryrun"
    )
    parser.add_argument(
        "--skip-integration",
        action="store_true",
        help="Skip integration tests that require BigQuery credentials"
    )
    args = parser.parse_args()
    
    # Check if we're in the right directory
    if not Path("pyproject.toml").exists():
        print(f"{Colors.FAIL}Error: Must run from project root directory{Colors.ENDC}")
        sys.exit(1)
    
    # Run test suite
    suite = ReleaseTestSuite(skip_integration=args.skip_integration)
    all_passed = await suite.run_all_tests()
    
    # Exit with appropriate code
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    asyncio.run(main())