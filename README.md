```
curl -u guest:guest http://localhost:15672/api/exchanges/%2F | jq '.[] | select(.name | contains("routix"))'
```