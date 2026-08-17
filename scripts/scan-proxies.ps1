# Fetches a geonode proxy list and tests each HTTP-capable candidate against the
# opencode free gateway, printing only those that respond 200 (not rate-limited).
# Usage: powershell -ExecutionPolicy Bypass -File scripts/scan-proxies.ps1 [-Limit 500] [-Workers 16]
param([int]$Limit = 500, [int]$Workers = 16)

$url = "https://proxylist.geonode.com/api/proxy-list?page=1&limit=$Limit&sort_by=responseTime&sort_type=asc"
Write-Host "Fetching $url"
$j = Invoke-RestMethod -Uri $url -TimeoutSec 60
$cands = @($j.data | Where-Object { "$($_.protocols)" -match 'http' } | ForEach-Object { "$($_.ip):$($_.port)".ToLowerInvariant() } | Sort-Object -Unique)
Write-Host "Candidates: $($cands.Count)"

$worker = @'
param([string[]]$Chunk)
$body = '{"model":"deepseek-v4-flash-free","messages":[{"role":"user","content":"hi"}],"max_tokens":3}'
function Test-Cand($hp){
  $req = [System.Net.HttpWebRequest]::Create('https://opencode.ai/zen/v1/chat/completions')
  $req.Method='POST'; $req.ContentType='application/json'; $req.Timeout=20000; $req.UserAgent='opencode/1.18.9'
  $wp = New-Object System.Net.WebProxy("http://$hp"); $req.Proxy = $wp
  $bytes=[System.Text.Encoding]::UTF8.GetBytes($body); $req.ContentLength=$bytes.Length
  try {
    $s=$req.GetRequestStream(); $s.Write($bytes,0,$bytes.Length); $s.Close()
    $resp=$req.GetResponse(); $sr=New-Object System.IO.StreamReader($resp.GetResponseStream()); [void]$sr.ReadToEnd(); $resp.Close()
    return "OK|$hp"
  } catch {
    $msg = "$($_.Exception.InnerException.Message)"
    if($msg -match '429'){ return "RATE|$hp" }
    if($msg -match '407'){ return "AUTH|$hp" }
    return "ERR|$hp"
  }
}
$r = [System.Collections.Generic.List[string]]::new()
foreach($hp in $Chunk){ $r.Add((Test-Cand $hp)) }
$r.ToArray()
'@

$steps = [Math]::Ceiling($cands.Count / $Workers)
$chunks = @(); for($i=0;$i -lt $Workers;$i++){ $chunks += ,@($cands | Select-Object -Skip ($i*$steps) -First $steps) }

$pool = [System.Management.Automation.Runspaces.RunspaceFactory]::CreateRunspacePool(1,$Workers)
$pool.Open()
$handles = @()
foreach($chunk in $chunks){
  if($chunk.Count -eq 0){ continue }
  $ps = [System.Management.Automation.PowerShell]::Create(); $ps.RunspacePool = $pool
  [void]$ps.AddScript($worker).AddArgument($chunk)
  $handles += ,@($ps,$ps.BeginInvoke())
}
$results = @()
foreach($h in $handles){
  try { foreach($o in $h[0].EndInvoke($h[1])){ $results += [string]$o } } catch {}
  $h[0].Dispose()
}
$pool.Close()

$ok = @($results | Where-Object { $_ -match '^OK\|' })
$rate = @($results | Where-Object { $_ -match '^RATE\|' }).Count
Write-Host "OK=$($ok.Count) RATE429=$rate tested=$($cands.Count)"
$ok | ForEach-Object { Write-Output ($_ -replace '^OK\|','') }
Write-Host "Paste the above host:port lines into PROXY_LIST."