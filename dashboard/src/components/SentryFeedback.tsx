import { useEffect } from "react";

import * as Sentry from "@sentry/react";

type FeedbackWidget = {
  appendToDom?: () => void;
  removeFromDom?: () => void;
};

type FeedbackIntegrationWithWidget = ReturnType<
  typeof Sentry.feedbackIntegration
> & {
  createWidget?: () => FeedbackWidget;
};

export default function SentryFeedback() {
  useEffect(() => {
    if (!import.meta.env.VITE_SENTRY_DSN) {
      return;
    }

    const feedback = Sentry.feedbackIntegration({
      colorScheme: "light",
      buttonLabel: "Report a bug",
      submitButtonLabel: "Send report",
      formTitle: "Report a bug",
    }) as FeedbackIntegrationWithWidget;

    if (!feedback.createWidget) {
      return;
    }

    const widget = feedback.createWidget();
    widget.appendToDom?.();

    return () => widget.removeFromDom?.();
  }, []);

  return null;
}
