import { TokenFeed } from "./components/TokenFeed";

export default function App() {
  return (
    <div className="min-h-screen bg-gray-100 p-6">
      <div className="max-w-7xl mx-auto">
        <div className="mb-6">
          <h1 className="text-2xl font-bold text-gray-900 mb-2">
            Token Feed
          </h1>
          <p className="text-gray-600">
            Monitor cryptocurrency tokens with real-time data
          </p>
        </div>

        <TokenFeed />
      </div>
    </div>
  );
}