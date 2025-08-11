"""Manual test script for mcp-bigquery-dryrun."""

import asyncio
from mcp_bigquery_dryrun.server import validate_sql, dry_run_sql

async def test_tools():
    # 1. SQL検証テスト - 有効なSQL
    print("Test 1: Valid SQL")
    result = await validate_sql("SELECT 1")
    print(f"Result: {result}")
    assert result["isValid"] == True
    
    # 2. SQL検証テスト - 無効なSQL
    print("\nTest 2: Invalid SQL")
    result = await validate_sql("SELECT FROM WHERE")
    print(f"Result: {result}")
    assert result["isValid"] == False
    assert "error" in result
    
    # 3. ドライランテスト
    print("\nTest 3: Dry run")
    result = await dry_run_sql("SELECT 1")
    print(f"Result: {result}")
    assert "totalBytesProcessed" in result
    assert "usdEstimate" in result
    
    # 4. パラメータ付きクエリ
    print("\nTest 4: Query with parameters")
    result = await validate_sql(
        "SELECT * FROM table WHERE id = @id",
        params={"id": "123"}
    )
    print(f"Result: {result}")
    
    print("\n✅ All tests passed!")

if __name__ == "__main__":
    asyncio.run(test_tools())