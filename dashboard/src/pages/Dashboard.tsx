import NewsTable from "@/components/NewsTable";
import SectionErrorBoundary from "@/components/SectionErrorBoundary";
import SentimentBar from "@/components/SentimentBar";
import TimeSeriesChart from "@/components/TimeSeriesChart";
import { useWebSocket } from "@/hooks/useWebSocket";

export default function Dashboard() {
  const { isConnected, lastMessage } = useWebSocket(
    import.meta.env.VITE_WS_URL,
  );

  return (
    <div className="space-y-8">
      <SectionErrorBoundary name="sentiment-bar">
        <SentimentBar isConnected={isConnected} />
      </SectionErrorBoundary>
      <SectionErrorBoundary name="time-series-chart">
        <TimeSeriesChart />
      </SectionErrorBoundary>
      <SectionErrorBoundary name="news-table">
        <NewsTable lastMessage={lastMessage} />
      </SectionErrorBoundary>
    </div>
  );
}
