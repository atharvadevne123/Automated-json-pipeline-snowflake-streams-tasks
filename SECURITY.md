# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.1.x   | Yes       |
| 1.0.x   | Yes       |
| < 1.0   | No        |

## Reporting a Vulnerability

**Please do NOT open a public GitHub issue for security vulnerabilities.**

Report vulnerabilities by emailing **devneatharva@gmail.com** with:

1. A clear description of the vulnerability
2. Steps to reproduce
3. Potential impact assessment
4. Any suggested fixes (optional)

### Response Timeline

| Stage | Target |
|-------|--------|
| Acknowledgement | Within 48 hours |
| Initial assessment | Within 5 business days |
| Fix / mitigation | Within 30 days for critical issues |
| Public disclosure | After fix is released |

## Security Best Practices

When deploying this pipeline:

- Store all credentials (`SNOWFLAKE_PASSWORD`, AWS keys) in a secrets manager, never in code or `.env` files committed to VCS.
- Use the principle of least privilege for Snowflake roles and S3 IAM policies.
- Enable Snowflake network policies to restrict connection sources.
- Rotate credentials at least every 90 days.
- Review the bundled `sample_amazon_reviews.json` before use in production — it is synthetic data only.

## Known Limitations

- The pipeline does not encrypt data at rest locally; rely on Snowflake's native encryption.
- SQL injection is not applicable to the bundled DDL scripts, but any dynamic SQL construction by downstream consumers must use parameterised queries.
