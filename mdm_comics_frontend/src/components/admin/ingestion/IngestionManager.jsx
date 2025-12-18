import React, { useState } from 'react';
import CoverIngestion from './CoverIngestion';
import ScannerApp from './ScannerApp';
import MatchReviewQueue from './match-review/MatchReviewQueue';
import ScanQueue from './ScanQueue';

const TABS = {
  COVER_INGESTION: 'Cover Ingestion',
  SCANNER: 'Scanner',
  SCAN_QUEUE: 'Scan Queue',
  MATCH_REVIEW: 'Match Review',
};

export default function IngestionManager() {
  const [activeTab, setActiveTab] = useState(TABS.COVER_INGESTION);

  const renderContent = () => {
    switch (activeTab) {
      case TABS.COVER_INGESTION:
        return <CoverIngestion />;
      case TABS.SCANNER:
        return <ScannerApp embedded />;
      case TABS.SCAN_QUEUE:
        return <ScanQueue />;
      case TABS.MATCH_REVIEW:
        return <MatchReviewQueue />;
      default:
        return <CoverIngestion />;
    }
  };

  return (
    <div className="ingestion-manager">
      <header className="flex border-b border-gray-700">
        {Object.values(TABS).map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            className={`px-4 py-2 text-sm font-medium ${
              activeTab === tab
                ? 'border-b-2 border-blue-500 text-white'
                : 'text-gray-400 hover:text-white'
            }`}
          >
            {tab}
          </button>
        ))}
      </header>
      <main className="p-4">
        {renderContent()}
      </main>
    </div>
  );
}
