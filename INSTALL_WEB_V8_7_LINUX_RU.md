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

## 7. Отправить свежий пакет с Windows

После запуска Robot V8.5:

```powershell
cd C:\elliot_robot_v8_5
.\.venv\Scripts\python.exe -X utf8 -u .\web_publisher.py --once
```

После этого обновите страницы «Обзор», «Рынок», «Анализы» и «Система».

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
