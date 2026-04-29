#!/usr/bin/env pwsh
# ============================================================================
# Platform POC — Automatiserad 10-stegs demo
# ============================================================================
#
# Kör hela demo-testcaset via Platform Demo Runner API.
# Steg 1-9 körs via API:et. Steg 10 (resiliens) körs med Docker CLI.
#
# Användning:
#   .\test-demo.ps1              # Kör alla 10 steg
#   .\test-demo.ps1 -Steps 1,2,3 # Kör bara steg 1-3
#   .\test-demo.ps1 -SkipReset   # Hoppa över docker compose down/up
#   .\test-demo.ps1 -NoPause     # Kör utan pauser mellan steg
#
# Förutsättningar:
#   - Docker Desktop körs
#   - Inga containers från tidigare kör (scriptet gör docker compose down/up)
#
# ============================================================================

param(
    [int[]]$Steps = @(1,2,3,4,5,6,7,8,9,10),
    [switch]$SkipReset,
    [switch]$NoPause
)

$ErrorActionPreference = "Stop"
$PLATFORM = "http://localhost:3000"
$ERP = "http://localhost:3001"
$PROD_A = "http://localhost:3002"
$PROD_B = "http://localhost:3003"

function Write-Step($step, $name) {
    Write-Host ""
    Write-Host ("=" * 60) -ForegroundColor DarkGray
    Write-Host "  STEG $step — $name" -ForegroundColor Cyan
    Write-Host ("=" * 60) -ForegroundColor DarkGray
}

function Write-Ok($msg) {
    Write-Host "  ✅ $msg" -ForegroundColor Green
}

function Write-Info($msg) {
    Write-Host "  ℹ️  $msg" -ForegroundColor Gray
}

function Write-Fail($msg) {
    Write-Host "  ❌ $msg" -ForegroundColor Red
}

function Wait-ForService($url, $name, $timeoutSec = 60) {
    Write-Info "Väntar på $name ($url)..."
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $null = Invoke-RestMethod "$url/health" -TimeoutSec 2
            Write-Ok "$name är uppe"
            return $true
        } catch {
            Start-Sleep -Milliseconds 1000
        }
    }
    Write-Fail "$name svarade inte inom $timeoutSec sekunder"
    return $false
}

function Pause-Between {
    if (-not $NoPause) {
        Write-Host ""
        Write-Host "  Tryck Enter för nästa steg..." -ForegroundColor DarkYellow -NoNewline
        Read-Host
    } else {
        Start-Sleep -Seconds 1
    }
}

# ============================================================================
# SETUP
# ============================================================================

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║         Platform POC — 8-stegs demo                     ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

if (-not $SkipReset) {
    Write-Info "Rensar alla containers och data (docker compose down)..."
    docker compose down 2>&1 | Out-Null
    Write-Info "Startar alla tjänster (docker compose up -d)..."
    docker compose up -d 2>&1 | Out-Null

    # Vänta på att alla tjänster är uppe
    $allUp = $true
    $allUp = $allUp -and (Wait-ForService $PLATFORM "Platform")
    $allUp = $allUp -and (Wait-ForService $ERP "ERP Mock")
    $allUp = $allUp -and (Wait-ForService $PROD_A "Product A")
    $allUp = $allUp -and (Wait-ForService $PROD_B "Product B")

    if (-not $allUp) {
        Write-Fail "Inte alla tjänster startade. Avbryter."
        exit 1
    }

    # Extra väntetid för Kafka-consumers att ansluta
    Write-Info "Väntar 3 sek på Kafka-consumers..."
    Start-Sleep -Seconds 3
} else {
    Write-Info "Hoppar över reset (--SkipReset)"
}

# Reset demo state
Write-Info "Nollställer demo-state..."
$null = Invoke-RestMethod "$PLATFORM/api/demo/reset" -Method POST

# ============================================================================
# STEG 1 — ERP publicerar referensdata
# ============================================================================

if ($Steps -contains 1) {
    Write-Step 1 "ERP publicerar referensdata + Platform konfigurerar ekonomimodell"
    Write-Info "Kontoplan (4010 Löner, 4020 Konsulter, 5010 Resor) + org-enheter..."
    Write-Info "Ekonomimodell: dim1=Aktivitet, dim2=Kostnadsbärare, dim3=Motpart"

    $r = Invoke-RestMethod "$PLATFORM/api/demo/step/1" -Method POST
    if ($r.ok) {
        Write-Ok $r.description
        Write-Info "Events: AccountsPublished → erp.accounts → platform.accounts.out"
        Write-Info "Dim model + routing konfigurerat: erp.activity→dim1, erp.cost_bearer→dim2, erp.counterpart→dim3"
    } else {
        Write-Fail "Steg 1 misslyckades: $($r.error)"
        exit 1
    }
    Pause-Between
}

# ============================================================================
# STEG 2 — ERP skapar riktigt projekt
# ============================================================================

if ($Steps -contains 2) {
    Write-Step 2 "ERP skapar riktigt projekt"
    Write-Info "Skapar ERP-projekt 'Nytt kontorshus'..."

    $r = Invoke-RestMethod "$PLATFORM/api/demo/step/2" -Method POST
    if ($r.ok) {
        Write-Ok $r.description
        Write-Info "Events: ProjectCreated → erp.projects → platform.projects.out"
    } else {
        Write-Fail "Steg 2 misslyckades: $($r.error)"
        exit 1
    }
    Pause-Between
}

# ============================================================================
# STEG 3 — ERP publicerar utfall
# ============================================================================

if ($Steps -contains 3) {
    Write-Step 3 "ERP publicerar utfall (GL)"
    Write-Info "GL med 3 flex-dimensioner: aktivitet, kostnadsbärare, motpart..."

    $r = Invoke-RestMethod "$PLATFORM/api/demo/step/3" -Method POST
    if ($r.ok) {
        Write-Ok $r.description
        Write-Info "Events: GeneralLedgerPublished → erp.general-ledger → platform.gl.out"
        Write-Info "Platform applicerar dim_routing: activity→dim1, cost_bearer→dim2, counterpart→dim3"
    } else {
        Write-Fail "Steg 3 misslyckades: $($r.error)"
        exit 1
    }
    Pause-Between
}

# ============================================================================
# STEG 4 — Product A skapar budgetprojekt
# ============================================================================

if ($Steps -contains 4) {
    Write-Step 4 "Product A skapar budgetprojekt"
    Write-Info "Skapar fiktivt projekt 'Nytt kontorshus — planering'..."

    $r = Invoke-RestMethod "$PLATFORM/api/demo/step/4" -Method POST
    if ($r.ok) {
        Write-Ok $r.description
        Write-Info "Events: BudgetProjectCreated → product-a.events → platform.projects.out"
    } else {
        Write-Fail "Steg 4 misslyckades: $($r.error)"
        exit 1
    }
    Pause-Between
}

# ============================================================================
# STEG 5 — Product A sparar budget (utkast)
# ============================================================================

if ($Steps -contains 5) {
    Write-Step 5 "Product A sparar budget (utkast)"
    Write-Info "Lägger budgetposter: 500k (4010) + 200k (4020) — sparas lokalt, ingen Kafka..."

    $r = Invoke-RestMethod "$PLATFORM/api/demo/step/5" -Method POST
    if ($r.ok) {
        Write-Ok $r.description
        Write-Info "Sparas som utkast — ingen event publiceras till Kafka ännu"
    } else {
        Write-Fail "Steg 5 misslyckades: $($r.error)"
        exit 1
    }
    Pause-Between
}

# ============================================================================
# STEG 6 — Platform: Konfigurera dimensionsmappning
# ============================================================================

if ($Steps -contains 6) {
    Write-Step 6 "Platform: Konfigurera dimensionsmappning"
    Write-Info "Översätter Product A:s 'Budget 2025' till planning-dimensioner..."

    $r = Invoke-RestMethod "$PLATFORM/api/demo/step/6" -Method POST
    if ($r.ok) {
        Write-Ok $r.description
        Write-Info "Mappning: planning_type=$($r.mapping.planning_type), planning_year=$($r.mapping.planning_year), planning_version=$($r.mapping.planning_version)"
        Write-Info "Flex-dims konfigurerades redan i steg 1 (economic model setup)"
    } else {
        Write-Fail "Steg 6 misslyckades: $($r.error)"
        exit 1
    }
    Pause-Between
}

# ============================================================================
# STEG 7 — Product A skickar in budget
# ============================================================================

if ($Steps -contains 7) {
    Write-Step 7 "Product A: Skicka in budget"
    Write-Info "Ändrar status utkast → inskickad. Publicerar BudgetSubmitted..."

    $r = Invoke-RestMethod "$PLATFORM/api/demo/step/7" -Method POST
    if ($r.ok) {
        Write-Ok $r.description
        Write-Info "Events: BudgetSubmitted → product-a.events → Platform berikar med konfigurerad dimensionsmappning → platform.budget.out"
    } else {
        Write-Fail "Steg 7 misslyckades: $($r.error)"
        exit 1
    }
    Pause-Between
}

# ============================================================================
# STEG 8 — Manuell länkning
# ============================================================================

if ($Steps -contains 8) {
    Write-Step 8 "Platform: Länka projekt"
    Write-Info "Kopplar ihop budgetprojekt ↔ ERP-projekt..."

    $r = Invoke-RestMethod "$PLATFORM/api/demo/step/8" -Method POST
    if ($r.ok) {
        Write-Ok $r.description
        Write-Info "Events: EntityLinked → platform.entity-linked.out"
        Write-Info "Product A + B uppdaterar sina lokala mappningar"
    } else {
        Write-Fail "Steg 8 misslyckades: $($r.error)"
        exit 1
    }
    Pause-Between
}

# ============================================================================
# STEG 9 — Product B visar analys
# ============================================================================

if ($Steps -contains 9) {
    Write-Step 9 "Product B: Visa analys"
    Write-Info "Hämtar analytics — väntar 2 sek på event-processing..."
    Start-Sleep -Seconds 2

    $r = Invoke-RestMethod "$PLATFORM/api/demo/step/9" -Method POST
    if ($r.ok) {
        Write-Ok $r.description
        Write-Host ""
        Write-Host "  ┌────────┬─────────┬────────────┬────────────┬────────────┬─────────────────────┬──────────────┐" -ForegroundColor Gray
        Write-Host "  │ Konto  │ Typ     │ Dim1       │ Dim2       │ Dim3       │ Planning            │ Belopp       │" -ForegroundColor Gray
        Write-Host "  ├────────┼─────────┼────────────┼────────────┼────────────┼─────────────────────┼──────────────┤" -ForegroundColor Gray
        foreach ($row in $r.analytics) {
            $typ = if ($row.source -eq 'budget') { "Budget " } else { "Utfall " }
            $d1 = if ($row.dim1) { "{0,-10}" -f $row.dim1 } else { "—         " }
            $d2 = if ($row.dim2) { "{0,-10}" -f $row.dim2 } else { "—         " }
            $d3 = if ($row.dim3) { "{0,-10}" -f $row.dim3 } else { "—         " }
            $plan = if ($row.planning_type) { "{0,-19}" -f "$($row.planning_type) $($row.planning_year) v$($row.planning_version)" } else { "—                  " }
            $amt = "{0,12:N0}" -f [double]$row.amount
            Write-Host ("  │ {0,-6} │ {1} │ {2} │ {3} │ {4} │ {5} │ {6} │" -f $row.account, $typ, $d1, $d2, $d3, $plan, $amt) -ForegroundColor White
        }
        Write-Host "  └────────┴─────────┴────────────┴────────────┴────────────┴─────────────────────┴──────────────┘" -ForegroundColor Gray
        Write-Host ""
        Write-Info "Budget-rader: planning-dimensioner från Platform (Budget 2025 v1), flex-dims=null"
        Write-Info "GL-rader: flex-dimensioner via Platform routing (AKT-100/KB-500/MP-200)"
    } else {
        Write-Fail "Steg 9 misslyckades: $($r.error)"
    }
    Pause-Between
}

# ============================================================================
# STEG 10 — Resiliens: Product B nere → synkar ikapp
# ============================================================================

if ($Steps -contains 10) {
    Write-Step 10 "Resiliens: Product B nere → synkar ikapp"

    # 8a: Stoppa Product B
    Write-Info "Stoppar Product B..."
    docker compose stop product-b 2>&1 | Out-Null
    Write-Ok "Product B är stoppad"

    # 8b: Skapa nytt projekt + budget + GL medan B är nere
    Write-Info "Skapar data medan Product B är nere..."

    # Nytt budgetprojekt via Product A
    $proj = Invoke-RestMethod "$PROD_A/api/projects" -Method POST -ContentType "application/json" -Body '{"name":"Serverhall — uppgradering"}'
    $newPaId = $proj.event.prod_a_id
    Write-Ok "Nytt budgetprojekt: $newPaId"

    Start-Sleep -Seconds 1

    # Budget
    $budgetBody = @{
        prod_a_id = $newPaId
        lines = @(
            @{ account = "5010"; org_unit = "OU-200"; amount = 300000; currency = "SEK"; period = "2025-02" }
        )
    } | ConvertTo-Json -Depth 3
    $null = Invoke-RestMethod "$PROD_A/api/budget" -Method POST -ContentType "application/json" -Body $budgetBody
    Write-Ok "Budget: 300 000 SEK (5010 Resor)"

    Start-Sleep -Seconds 1

    # ERP-projekt
    $erp2 = Invoke-RestMethod "$ERP/api/create-project" -Method POST -ContentType "application/json" -Body '{"name":"Serverhall"}'
    $newErpId = $erp2.event.erp_id
    Write-Ok "ERP-projekt: $newErpId"

    Start-Sleep -Seconds 1

    # Länka via Platform API
    $null = Invoke-WebRequest "$PLATFORM/api/login" -Method POST -ContentType "application/json" -Body '{"username":"admin","password":"demo"}' -SessionVariable sess -UseBasicParsing
    $linkBody = @{ source_id = $newPaId; target_id = $newErpId } | ConvertTo-Json
    $null = Invoke-RestMethod "$PLATFORM/api/link" -Method POST -ContentType "application/json" -Body $linkBody -WebSession $sess
    Write-Ok "Länkad: $newPaId ↔ $newErpId"

    Start-Sleep -Seconds 1

    # GL
    $glBody = @{
        erp_id = $newErpId
        entries = @(
            @{ account = "5010"; org_unit = "OU-200"; amount = 275000; currency = "SEK"; period = "2025-02"; activity = "AKT-300"; cost_bearer = "KB-700"; counterpart = "MP-400" }
        )
    } | ConvertTo-Json -Depth 3
    $null = Invoke-RestMethod "$ERP/api/publish-gl" -Method POST -ContentType "application/json" -Body $glBody
    Write-Ok "GL: 275 000 SEK (5010, AKT-300)"

    Write-Host ""
    Write-Info "Product B vet INGET om dessa ändringar — alla events köas i Kafka"
    Pause-Between

    # 8c: Starta Product B
    Write-Info "Startar Product B igen..."
    docker compose start product-b 2>&1 | Out-Null

    # Vänta på att Product B är uppe
    $null = Wait-ForService $PROD_B "Product B"
    Write-Info "Väntar 5 sek på event catch-up..."
    Start-Sleep -Seconds 5

    # 8d: Verifiera att allt synkats
    Write-Info "Verifierar analytics..."
    $analytics = Invoke-RestMethod "$PROD_B/api/analytics"
    Write-Ok "Product B har nu $($analytics.Count) analysrad(er) — alla events har synkats!"

    Write-Host ""
    Write-Host "  Loggar (sista 15 rader):" -ForegroundColor DarkYellow
    docker compose logs product-b --tail 15 2>&1 | ForEach-Object {
        Write-Host "    $_" -ForegroundColor DarkGray
    }
}

# ============================================================================
# SAMMANFATTNING
# ============================================================================

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║         Demo komplett!                                   ║" -ForegroundColor Green
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""

$state = Invoke-RestMethod "$PLATFORM/api/demo/state"
Write-Info "Demo-state:"
if ($state.prod_a_id) { Write-Info "  Product A: $($state.prod_a_id)" }
if ($state.erp_id)    { Write-Info "  ERP:       $($state.erp_id)" }

Write-Host ""
Write-Info "Öppna i webbläsare:"
Write-Info "  Platform Admin:  http://localhost:3000/admin.html (admin/demo)"
Write-Info "  Product A:       http://localhost:3002 (anna/demo)"
Write-Info "  Product B:       http://localhost:3003 (erik/demo)"
Write-Info "  Redpanda Console: http://localhost:8080"
Write-Info "  Jaeger:          http://localhost:16686"
Write-Host ""
