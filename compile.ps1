# FlexQL build script
# Usage: ..\compile.ps1

$ErrorActionPreference = 'Stop'

Write-Host "Building FlexQL server..."
g++ -O3 -std=c++17 -march=native -pthread `
    flexql_server.cpp `
    -o server.exe

Write-Host "Building benchmark client..."
g++ -O2 -std=c++17 `
    flexql.cpp benchmark_flexql.cpp `
    -o benchmark.exe

Write-Host ""
Write-Host "Done. Run in two terminals:"
Write-Host "  Terminal 1:  .\server.exe"
Write-Host "  Terminal 2:  .\benchmark.exe --unit-test"
Write-Host "  Terminal 2:  .\benchmark.exe <row_count>"
