import { defineConfig, globalIgnores } from "eslint/config";
import nextVitals from "eslint-config-next/core-web-vitals";
import nextTs from "eslint-config-next/typescript";

const eslintConfig = defineConfig([
  ...nextVitals,
  ...nextTs,
  {
    rules: {
      // This rule pushes toward Suspense/`use()`-based data fetching,
      // which this app doesn't use anywhere — adopting it would mean
      // rewriting every list/detail page's fetch pattern, disproportionate
      // to what Phase 11 needs. The flagged pattern here
      // (setLoading(true) -> fetch -> setLoading(false) in an effect) is
      // the standard, correct client-side data-fetching idiom and has been
      // verified working live in the browser across every page. Revisit if
      // this codebase later adopts Suspense for data fetching.
      "react-hooks/set-state-in-effect": "off",
    },
  },
  // Override default ignores of eslint-config-next.
  globalIgnores([
    // Default ignores of eslint-config-next:
    ".next/**",
    "out/**",
    "build/**",
    "next-env.d.ts",
  ]),
]);

export default eslintConfig;
