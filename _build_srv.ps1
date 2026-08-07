$wd = 'C:\Users\caohua\Desktop\IDEAproject\FishGrabbingServer'
Set-Location $wd
$javac = 'C:\Program Files\Java\jdk-26.0.1\bin\javac.exe'
$jar = 'C:\Program Files\Java\jdk-26.0.1\bin\jar.exe'
& $javac -encoding UTF-8 -cp 'lib\mysql-connector-j-8.0.33.jar' -sourcepath 'src' -d 'out\production\FishGrabbingServer' 'src\server\Main.java' *> compile_srv.log
$code = $LASTEXITCODE
if ($code -ne 0) { Write-Host 'COMPILE FAILED' ; Get-Content compile_srv.log -Head 30 ; exit 1 }
Write-Host 'COMPILE OK'
& $jar cfm 'FishGrabbingServer.jar' 'MANIFEST.MF' -C 'out\production\FishGrabbingServer' . *> jar_srv.log
$code = $LASTEXITCODE
if ($code -ne 0) { Write-Host 'JAR FAILED' ; Get-Content jar_srv.log -Head 20 ; exit 1 }
$dst = Get-Item 'FishGrabbingServer.jar'
Write-Host ('size={0} mtime={1}' -f $dst.Length, $dst.LastWriteTime)
