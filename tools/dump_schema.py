#!/usr/bin/env python3
"""Fetch AMC's anonymous GraphQL introspection schema through configured egress."""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import amc_seat_monitor as monitor  # noqa: E402


INTROSPECTION_QUERY = r"""
query IntrospectionQuery {
  __schema {
    queryType { name }
    mutationType { name }
    subscriptionType { name }
    types { ...FullType }
    directives {
      name description locations
      args { ...InputValue }
    }
  }
}
fragment FullType on __Type {
  kind name description
  fields(includeDeprecated: true) {
    name description
    args { ...InputValue }
    type { ...TypeRef }
    isDeprecated deprecationReason
  }
  inputFields { ...InputValue }
  interfaces { ...TypeRef }
  enumValues(includeDeprecated: true) {
    name description isDeprecated deprecationReason
  }
  possibleTypes { ...TypeRef }
}
fragment InputValue on __InputValue {
  name description
  type { ...TypeRef }
  defaultValue
}
fragment TypeRef on __Type {
  kind name
  ofType { kind name ofType { kind name ofType { kind name ofType { kind name } } } }
}
"""


def main() -> int:
    output = ROOT / "docs" / "amc-graphql-introspection.json"
    client = monitor.GraphQLClient(monitor.load_config())
    data = client.query(INTROSPECTION_QUERY, {})
    schema = data["__schema"]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps({"data": {"__schema": schema}}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    public_types = [t for t in schema["types"] if not t["name"].startswith("__")]
    print(f"Wrote {output} ({len(public_types)} public types).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
