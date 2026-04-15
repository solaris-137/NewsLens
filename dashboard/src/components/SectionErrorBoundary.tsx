import type { ReactNode } from "react";

import * as Sentry from "@sentry/react";

interface Props {
  name: string;
  children: ReactNode;
}

export default function SectionErrorBoundary({ name, children }: Props) {
  return (
    <Sentry.ErrorBoundary
      fallback={
        <div className="rounded-xl border border-red-200 bg-red-50 p-6 text-sm text-red-700">
          The {name} section failed to load. This error has been reported.
        </div>
      }
      beforeCapture={(scope) => scope.setTag("section", name)}
    >
      {children}
    </Sentry.ErrorBoundary>
  );
}
