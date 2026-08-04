# Security policy

## Reporting a vulnerability

Please use GitHub's private vulnerability reporting or a private Security
Advisory for this repository. Do not open a public issue containing a secret,
exploit, private deployment detail, or personal data.

Include the affected component, reproduction steps, expected impact, and any
suggested mitigation. Use synthetic values in screenshots and logs.

## Scope

Security boundaries include guild isolation, PostgreSQL row-level security,
service-specific database roles, AMC request serialization, outbound-only
networking, webhook separation, and secret redaction.

The project does not operate a hosted service and cannot rotate credentials or
patch deployments run by other people. If you accidentally expose a credential,
revoke it with the issuing provider immediately before reporting the code path
that allowed the exposure.
