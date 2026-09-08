# run_tests.ps1
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Running ACID Integration Tests  " -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "NOTE: Ensure both Wallet Service (port 8000) and Transactions Service (port 8080) are running." -ForegroundColor Yellow
Write-Host ""

# Run the integration tests (they are behind the "integration" build tag).
# Timeout raised to 600s: each test ends with a CDC convergence wait.
go test -v -tags=integration -count=1 -timeout 600s ./tests/

Write-Host ""
Write-Host "To run a specific test, use:" -ForegroundColor Green
Write-Host "go test -v -tags=integration -count=1 -timeout 600s ./tests/ -run TestName" -ForegroundColor White
