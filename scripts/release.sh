#!/bin/bash
# Release script for github.com/sllt/pi
# Usage: ./scripts/release.sh [version]
# Example: ./scripts/release.sh v0.1.0

set -e

VERSION=${1:-v0.1.0}

echo "🚀 Releasing $VERSION for github.com/sllt/pi"
echo ""

# Step 1: Commit changes (if any)
if [[ -n $(git status --porcelain) ]]; then
    echo "📝 Committing changes..."
    git add .
    git commit -m "Rename module to github.com/sllt/pi

- Changed all import paths from gofr.dev to github.com/sllt/pi
- Updated go.mod, go.work, and documentation
- Prepared for initial release $VERSION"
fi

# Step 2: Create main module tag
echo "🏷️  Creating main module tag: $VERSION"
git tag -a "$VERSION" -m "Release $VERSION"

# Step 3: Create submodule tags
SUBMODULES=(
    "pkg/pi/datasource/arangodb"
    "pkg/pi/datasource/cassandra"
    "pkg/pi/datasource/clickhouse"
    "pkg/pi/datasource/couchbase"
    "pkg/pi/datasource/dbresolver"
    "pkg/pi/datasource/dgraph"
    "pkg/pi/datasource/elasticsearch"
    "pkg/pi/datasource/file/azure"
    "pkg/pi/datasource/file/ftp"
    "pkg/pi/datasource/file/gcs"
    "pkg/pi/datasource/file/s3"
    "pkg/pi/datasource/file/sftp"
    "pkg/pi/datasource/influxdb"
    "pkg/pi/datasource/kv-store/badger"
    "pkg/pi/datasource/kv-store/dynamodb"
    "pkg/pi/datasource/kv-store/nats"
    "pkg/pi/datasource/mongo"
    "pkg/pi/datasource/opentsdb"
    "pkg/pi/datasource/oracle"
    "pkg/pi/datasource/pubsub/eventhub"
    "pkg/pi/datasource/pubsub/nats"
    "pkg/pi/datasource/pubsub/sqs"
    "pkg/pi/datasource/scylladb"
    "pkg/pi/datasource/solr"
    "pkg/pi/datasource/surrealdb"
)

echo "🏷️  Creating submodule tags..."
for module in "${SUBMODULES[@]}"; do
    tag="${module}/${VERSION}"
    echo "   - $tag"
    git tag -a "$tag" -m "Release ${module} $VERSION"
done

echo ""
echo "✅ All tags created!"
echo ""
echo "📋 Tags created:"
git tag -l "*${VERSION}*" | head -30
echo ""

# Step 4: Instructions for pushing
echo "🔄 To push to GitHub, run:"
echo ""
echo "   # Add remote (if not exists)"
echo "   git remote add origin git@github.com:sllt/pi.git"
echo ""
echo "   # Push code and all tags"
echo "   git push -u origin HEAD"
echo "   git push origin --tags"
echo ""
echo "   # Or push everything at once"
echo "   git push -u origin HEAD --tags"
