#!/usr/bin/env bash
# Configura Dremio la primera vez. Se puede repetir sin problema, no duplica nada:
#   1. Crea el usuario administrador: admin / admin12345
#   2. Crea la fuente "nessie" (el catálogo Iceberg) apuntando a MinIO
# Se usa así:  bash scripts/dremio_init.sh
set -u
DREMIO="${DREMIO_URL:-http://localhost:9047}"
USUARIO="admin"
CLAVE="admin12345"     # Dremio pide mínimo 8 caracteres con letras y números

echo "Esperando a Dremio en $DREMIO ..."
for _ in $(seq 1 60); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' -m 5 "$DREMIO/")" = "200" ] && break
  sleep 5
done

echo "1) Usuario administrador"
curl -s -m 30 -X PUT "$DREMIO/apiv2/bootstrap/firstuser" \
  -H "Authorization: _dremionull" -H "Content-Type: application/json" \
  -d "{\"userName\":\"$USUARIO\",\"firstName\":\"Admin\",\"lastName\":\"Lakehouse\",\"email\":\"admin@example.com\",\"createdAt\":$(date +%s)000,\"password\":\"$CLAVE\"}" \
  | grep -q '"userName"' && echo "   creado" || echo "   ya existia (o ya estaba configurado)"

TOKEN=$(curl -s -m 30 -X POST "$DREMIO/apiv2/login" -H "Content-Type: application/json" \
  -d "{\"userName\":\"$USUARIO\",\"password\":\"$CLAVE\"}" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
[ -z "$TOKEN" ] && { echo "No se pudo iniciar sesion en Dremio"; exit 1; }

echo "2) Fuente Nessie"
RESP=$(curl -s -m 60 -X POST "$DREMIO/api/v3/catalog" \
  -H "Authorization: _dremio$TOKEN" -H "Content-Type: application/json" -d '{
  "entityType": "source",
  "name": "nessie",
  "type": "NESSIE",
  "config": {
    "nessieEndpoint": "http://nessie:19120/api/v2",
    "nessieAuthType": "NONE",
    "credentialType": "ACCESS_KEY",
    "awsAccessKey": "admin",
    "awsAccessSecret": "password",
    "awsRootPath": "warehouse",
    "secure": false,
    "propertyList": [
      {"name": "fs.s3a.path.style.access", "value": "true"},
      {"name": "fs.s3a.endpoint", "value": "minio:9000"},
      {"name": "dremio.s3.compat", "value": "true"}
    ]
  }
}')
echo "$RESP" | grep -q '"status":"good"' && echo "   fuente 'nessie' creada" || echo "   (ya existia o revisar en la UI) $(echo "$RESP" | head -c 200)"
echo "Listo. Dremio: $DREMIO  (usuario $USUARIO / $CLAVE)"
