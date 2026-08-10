# run_tests.ps1
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Running ACID Integration Tests  " -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "NOTE: Ensure both Wallet Service (port 8000) and Transactions Service (port 8080) are running." -ForegroundColor Yellow
Write-Host ""

# Run the integration tests
go test -v -tags=integration -count=1 -timeout 120s ./tests/

Write-Host ""
Write-Host "To run a specific test, use:" -ForegroundColor Green
Write-Host "go test -v -tags=integration -count=1 -timeout 120s ./tests/ -run TestName" -ForegroundColor White
