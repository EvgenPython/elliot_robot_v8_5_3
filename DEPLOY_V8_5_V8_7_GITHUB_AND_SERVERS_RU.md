# V8.5 + V8.7: GitHub, Windows-робот и Linux-веб

Используются уже указанные репозитории:

- Robot: `https://github.com/EvgenPython/elliot_robot_v8_4.git`
- Web: `https://github.com/EvgenPython/waveframe_web_v8_6.git`

Названия репозиториев остаются прежними, версия внутри обновляется до V8.5 и
V8.7. Секретные конфигурации и runtime-данные в GitHub не загружаются.

## A. Загрузить Robot V8.5 в GitHub

Скачайте `elliot_robot_v8_5.zip` и `waveframe_web_v8_7.zip` в Downloads.
На локальном ПК, в PowerShell, выполните:

```powershell
New-Item -ItemType Directory -Path D:\releases -Force | Out-Null
New-Item -ItemType Directory -Path D:\git -Force | Out-Null
Expand-Archive -LiteralPath "$env:USERPROFILE\Downloads\elliot_robot_v8_5.zip" -DestinationPath D:\releases -Force
Expand-Archive -LiteralPath "$env:USERPROFILE\Downloads\waveframe_web_v8_7.zip" -DestinationPath D:\releases -Force
Test-Path D:\releases\elliot_robot_v8_5\runner.py
Test-Path D:\releases\waveframe_web_v8_7\manage.py
```

Две последние команды должны вернуть `True`. Внутри архивов уже есть корневая
папка проекта. Не добавляйте ещё одну папку при распаковке.

Для загрузки робота:

```powershell
$RobotSource = "D:\releases\elliot_robot_v8_5"
$RobotGit = "D:\git\elliot_robot_v8_5"
git clone https://github.com/EvgenPython/elliot_robot_v8_4.git $RobotGit
if (!(Test-Path "$RobotSource\runner.py")) { throw "Неверная папка RobotSource" }
Get-ChildItem $RobotSource -Force | Copy-Item -Destination $RobotGit -Recurse -Force
Set-Location $RobotGit
Get-Content .\VERSION
git status --short
git ls-files config/account.json config/anthropic.json config/web_export.json state analysis_archive logs
```

Последняя команда не должна вывести секретные/runtime-файлы. Затем:

```powershell
git add -A
git commit -m "Release WaveFrame Robot V8.5"
git push origin main
git log -1 --oneline
```

Если Git сообщает `nothing to commit`, проверьте `$RobotSource`: внутри него
должен лежать `runner.py`, а не ещё одна вложенная папка.

## B. Загрузить Web V8.7 в GitHub

Распакуйте второй архив, например в `D:\releases\waveframe_web_v8_7`.

```powershell
$WebSource = "D:\releases\waveframe_web_v8_7"
$WebGit = "D:\git\waveframe_web_v8_7"
git clone https://github.com/EvgenPython/waveframe_web_v8_6.git $WebGit
if (!(Test-Path "$WebSource\manage.py")) { throw "Неверная папка WebSource" }
Get-ChildItem $WebSource -Force | Copy-Item -Destination $WebGit -Recurse -Force
Set-Location $WebGit
Get-Content .\VERSION
git status --short
git ls-files .env db.sqlite3 staticfiles
```

Последняя команда не должна вывести `.env`, локальную БД или `staticfiles`.
Затем:

```powershell
git add -A
git commit -m "Release WaveFrame Web V8.7"
git push origin main
git log -1 --oneline
```


Сначала завершите push обоих проектов. Следующие команды выполняются уже на
серверах, а не в терминале локального PyCharm. При ошибке команды остановитесь
на ней; не продолжайте следующий блок.


# Обновление WaveFrame Web V8.6 -> V8.7 на Ubuntu

Команды рассчитаны на действующий сервер `waveframe.top` с путём
`/opt/robot-elliot-web/current`, PostgreSQL, Nginx и systemd-сервисом
`robot-elliot-web`. База, пользователь admin, engine token, HTTPS и env-файл
сохраняются. Базу удалять, создавать заново или повторно выполнять
`createsuperuser` не нужно.

## 1. Войти на сервер

В Windows PowerShell:

```powershell
ssh administrator@155.117.40.209
```

На Ubuntu:

```bash
sudo -i
systemctl is-active robot-elliot-web
systemctl is-active postgresql
systemctl is-active nginx
```

Все три результата должны быть `active`.

## 2. Задать пути и сделать три резервные копии

Вводите команды по одной строке:

```bash
BACKUP_TS=$(date +%Y%m%d_%H%M%S)
APP_ROOT=/opt/robot-elliot-web
BACKUP_DIR=$APP_ROOT/backups
RELEASE_DIR=$APP_ROOT/releases/v8.7_$BACKUP_TS
PREVIOUS_REAL=$(readlink -f $APP_ROOT/current)
umask 077
install -d -m 0700 $BACKUP_DIR
install -d -m 0755 $APP_ROOT/releases
set -a
source /etc/robot-elliot-web.env
set +a
sudo -u postgres pg_dump -Fc "$POSTGRES_DB" > "$BACKUP_DIR/${POSTGRES_DB}_${BACKUP_TS}.dump"
tar -czf "$BACKUP_DIR/web_code_${BACKUP_TS}.tar.gz" -C "$PREVIOUS_REAL" .
install -m 0600 /etc/robot-elliot-web.env "$BACKUP_DIR/environment_${BACKUP_TS}.env"
ls -lh "$BACKUP_DIR/${POSTGRES_DB}_${BACKUP_TS}.dump" "$BACKUP_DIR/web_code_${BACKUP_TS}.tar.gz" "$BACKUP_DIR/environment_${BACKUP_TS}.env"
```

Каждый из трёх файлов должен существовать и иметь ненулевой размер.

## 3. Скачать новый релиз из GitHub

После push V8.7 в указанный репозиторий:

```bash
git clone --depth 1 https://github.com/EvgenPython/waveframe_web_v8_6.git "$RELEASE_DIR"
test -f "$RELEASE_DIR/manage.py" && echo "MANAGE.PY: OK"
cat "$RELEASE_DIR/VERSION"
git -C "$RELEASE_DIR" rev-parse --short HEAD
```

Версия должна быть `8.7.0`.

## 4. Подготовить код и выполнить проверки до переключения

```bash
chown -R root:www-data "$RELEASE_DIR"
chmod -R g+rX "$RELEASE_DIR"
chmod -R o-rwx "$RELEASE_DIR"
/opt/robot-elliot-web/.venv/bin/python -m pip install --upgrade pip
/opt/robot-elliot-web/.venv/bin/python -m pip install -r "$RELEASE_DIR/requirements.txt"
install -d -o robotweb -g www-data -m 0750 "$RELEASE_DIR/staticfiles"
cd "$RELEASE_DIR"
setpriv --reuid=robotweb --regid=www-data --init-groups /opt/robot-elliot-web/.venv/bin/python "$RELEASE_DIR/manage.py" check
setpriv --reuid=robotweb --regid=www-data --init-groups env DJANGO_ENV=development DB_ENGINE=sqlite DJANGO_ALLOWED_HOSTS=localhost,127.0.0.1,testserver DJANGO_SECURE_SSL_REDIRECT=false /opt/robot-elliot-web/.venv/bin/python "$RELEASE_DIR/manage.py" test terminal
setpriv --reuid=robotweb --regid=www-data --init-groups /opt/robot-elliot-web/.venv/bin/python "$RELEASE_DIR/manage.py" migrate --noinput
setpriv --reuid=robotweb --regid=www-data --init-groups /opt/robot-elliot-web/.venv/bin/python "$RELEASE_DIR/manage.py" collectstatic --noinput
```

Тесты используют отдельную временную SQLite в памяти, не рабочую PostgreSQL.
Ожидается `Ran 39 tests` и `OK`. Команда migrate должна сообщить, что новых
миграций нет; даже если Django повторно проверит старые миграции, данные не
удаляются.

## 5. Переключить release и проверить сайт

```bash
systemctl stop robot-elliot-web
ln -sfn "$RELEASE_DIR" "$APP_ROOT/current"
systemctl start robot-elliot-web
systemctl is-active robot-elliot-web
readlink -f "$APP_ROOT/current"
curl --unix-socket /run/robot-elliot-web/gunicorn.sock -H "Host: waveframe.top" -H "X-Forwarded-Proto: https" http://localhost/healthz
curl -fsS https://waveframe.top/healthz
journalctl -u robot-elliot-web --since "5 minutes ago" --no-pager
```

Обе проверки health должны вернуть:

```json
{"ok": true, "database": "ok"}
```

Не заменяйте `/etc/robot-elliot-web.env`, Nginx-конфигурацию или существующий
systemd drop-in с `XDG_RUNTIME_DIR`: они уже настроены и переживают release.

## 6. Проверить сохранность данных

```bash
setpriv --reuid=robotweb --regid=www-data --init-groups /opt/robot-elliot-web/.venv/bin/python "$APP_ROOT/current/manage.py" shell -c "from terminal.models import AnalysisEvent; from django.contrib.auth import get_user_model; print('ANALYSES:', AnalysisEvent.objects.count()); print('USERS:', get_user_model().objects.count())"
```

Количество анализов не должно уменьшиться, `USERS` должно остаться не меньше 1.

## Быстрый rollback к V8.6

Переменная `$PREVIOUS_REAL` действует в той же root-сессии:

```bash
systemctl stop robot-elliot-web
ln -sfn "$PREVIOUS_REAL" "$APP_ROOT/current"
systemctl start robot-elliot-web
curl -fsS https://waveframe.top/healthz
```

V8.7 не добавляет миграций, поэтому для такого rollback восстановление дампа
PostgreSQL не требуется.

# Чистая установка WaveFrame Robot V8.5 на Windows-сервер

Инструкция рассчитана на разрешённую владельцем чистую замену V8.4 в выходной
день без открытых позиций и ордеров. Старые `state`, `analysis_archive` и
`logs` намеренно не переносятся: именно в старом состоянии застряла отменённая
pending-заявка. История остаётся в PostgreSQL веб-сервера.

## 1. Остановить старые процессы

PowerShell от имени пользователя, который запускает MT5:

```powershell
$OldRoot = "C:\elliot_robot_v8_4"
$NewRoot = "C:\elliot_robot_v8_5"
$ErrorActionPreference = "Stop"

Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match "runner\.py|web_publisher\.py" -and $_.Name -match "^python(w)?\.exe$" } | Select-Object ProcessId, Name, CommandLine | Format-List
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match "runner\.py|web_publisher\.py" -and $_.Name -match "^python(w)?\.exe$" } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
```

Проверка должна ничего не вывести:

```powershell
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match "runner\.py|web_publisher\.py" -and $_.Name -match "^python(w)?\.exe$" } | Select-Object ProcessId, CommandLine
```

## 2. Получить новый код

После загрузки релиза в указанный GitHub-репозиторий:

```powershell
git clone https://github.com/EvgenPython/elliot_robot_v8_4.git $NewRoot
Set-Location $NewRoot
git log -1 --oneline
Test-Path .\runner.py
```

Последняя команда должна вернуть `True`.

## 3. Создать окружение Python 3.11

```powershell
Set-Location $NewRoot
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r .\requirements.txt
```

Если `py -3.11` не найден, сначала выполните `py -0p` и используйте путь к уже
установленному 64-bit Python 3.11.

## 4. Перенести только три секретных конфигурации

```powershell
Copy-Item "$OldRoot\config\account.json" "$NewRoot\config\account.json" -Force
Copy-Item "$OldRoot\config\anthropic.json" "$NewRoot\config\anthropic.json" -Force
Copy-Item "$OldRoot\config\web_export.json" "$NewRoot\config\web_export.json" -Force
```

Не копируйте `execution.json`, `position_monitor.json`, `instruments.json`,
`state`, `analysis_archive` и `logs`: используйте новые файлы V8.5 и чистое
состояние.

```powershell
Get-Item .\config\account.json, .\config\anthropic.json, .\config\web_export.json, .\config\execution.json, .\config\position_monitor.json, .\config\instruments.json | Select-Object Name, Length, LastWriteTime
```

## 5. Бесплатные проверки

`check_trading_ready.py` подключается к MT5, но ордер не отправляет.

```powershell
Set-Location $NewRoot
.\.venv\Scripts\python.exe -X utf8 -m unittest discover -p "test_*.py" -v
.\.venv\Scripts\python.exe -X utf8 .\check_trading_ready.py
```

Ожидается `Ran 101 tests` и `OK` у тестов и `[READY]` у проверки MT5. Перед запуском убедитесь,
что `config\execution.json` действительно содержит желаемый режим торговли.

## 6. Проверить связь с вебом одним пакетом

```powershell
.\.venv\Scripts\python.exe -X utf8 -u .\web_publisher.py --once
```

Должно появиться сообщение об отправке runtime state без ошибки HTTP.

## 7. Запустить ровно один runner и один publisher

```powershell
Start-Process powershell.exe -ArgumentList @("-NoExit", "-ExecutionPolicy", "Bypass", "-File", "$NewRoot\run_runner.ps1")
Start-Process cmd.exe -ArgumentList @("/k", "$NewRoot\run_web_publisher.bat")
```

Повторный экземпляр V8.5 для того же инструмента будет отклонён lock.
Старые версии нужно остановить командами выше: их publisher ещё не знает
про новый общий lock. Если раньше настраивал автозапуск V8.4 в планировщике,
отключи старую задачу, чтобы она не запустила старый код после перезагрузки.

Проверка:

```powershell
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match "runner\.py|web_publisher\.py" -and $_.Name -match "^python(w)?\.exe$" } | Select-Object ProcessId, Name, CommandLine | Format-List
Get-Content (Get-ChildItem "$NewRoot\logs\runner_*.log" | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName -Encoding UTF8 -Tail 60
Get-Content "$NewRoot\logs\web_publisher.log" -Encoding UTF8 -Tail 40
```

В Windows иногда один логический Python-процесс виден как launcher и дочерний
`python.exe`; это допустимо. Недопустимы две отдельные команды runner или две
команды publisher из разных каталогов.

## 8. Аварийная остановка

```powershell
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match "runner\.py|web_publisher\.py" -and $_.Name -match "^python(w)?\.exe$" } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
```


# После установки

1. На Windows оставьте открытыми терминал MT5, окно runner и окно publisher.
   Закрытие RDP крестиком сохраняет сессию; выход из учётной записи Windows
   завершит процессы. Эти команды запускают программы сейчас и не создают
   автоматический запуск после перезагрузки сервера.
2. В браузере откройте https://waveframe.top и нажмите Ctrl+F5.
3. Проверьте «Передача данных», «Робот», явный режим торговли и время снимка.
4. Старые FVG без новой проверки скрываются. Новые зоны появятся после
   успешного FULL или POSITION_REVIEW робота V8.5, когда доступны свежие
   рыночные данные и разрешён анализ. Выходной день сам по себе не запускает Claude.
5. В истории ожидайте SCOUT на новых H1 в рабочем окне; при позиции —
   POSITION_SCOUT на M15 и POSITION_REVIEW на новой H1. Полный анализ не обязан
   повторяться каждый час: Scout сохраняет промежуточную проверку.

Серверы удалённо в ходе подготовки релиза не менялись. Автотесты не отправляли
ордера и не делали оплачиваемых запросов Claude. Их реальную связку с MT5
нужно проверить по журналу после установки. Отсутствие всех возможных ошибок
и точность будущей волновой разметки эти тесты не гарантируют.
