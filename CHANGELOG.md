# Changelog for konserve-ddb-s3

## 0.1.2 (unreleased)

**Breaking changes from 0.1.1**

* Change DynamoDB table format to use a two-level key:
  `tag` for the HASH key, and `key` for a RANGE key. This
  enables iterating DynamoDB keys in order.
* Change how S3 keys are encoded: it is now
  `<encoded-tag>.<encoded-key>`. This allows iteration of
  keys with the same tag in order.

## 0.1.1

* First semi-stable release.