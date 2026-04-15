import NewsTable from "@/components/NewsTable";
import SentimentBar from "@/components/SentimentBar";
import TimeSeriesChart from "@/components/TimeSeriesChart";
import { useWebSocket } from "@/hooks/useWebSocket";

export default function Dashboard() {
  const { isConnected, lastMessage } = useWebSocket(
    import.meta.env.VITE_WS_URL,
  );

  return (
    <div className="space-y-8">
      <SentimentBar isConnected={isConnected} />
      <TimeSeriesChart />
      <NewsTable lastMessage={lastMessage} />
    </div>
  );
}
