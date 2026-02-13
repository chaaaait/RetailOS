// AI Decision Feed component displaying recent AI-driven decisions

import type { AIDecision } from '@/types/kpi';

interface AIDecisionFeedProps {
    data: AIDecision[];
}

export default function AIDecisionFeed({ data }: AIDecisionFeedProps) {
    if (!data || data.length === 0) {
        return (
            <div className="bg-gray-800 rounded-lg p-6 border border-gray-700">
                <h3 className="text-lg font-semibold text-white mb-4">AI Decision Feed</h3>
                <p className="text-gray-500">No AI decisions available</p>
            </div>
        );
    }

    const getStatusColor = (status: string) => {
        switch (status) {
            case 'executed':
                return 'bg-green-500/20 text-green-400 border-green-500/30';
            case 'approved':
                return 'bg-blue-500/20 text-blue-400 border-blue-500/30';
            case 'pending':
                return 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30';
            case 'rejected':
                return 'bg-red-500/20 text-red-400 border-red-500/30';
            default:
                return 'bg-gray-500/20 text-gray-400 border-gray-500/30';
        }
    };

    const getConfidenceColor = (confidence: number) => {
        if (confidence >= 0.8) return 'text-green-400';
        if (confidence >= 0.6) return 'text-yellow-400';
        return 'text-red-400';
    };

    const formatTimestamp = (timestamp: string) => {
        const date = new Date(timestamp);
        const now = new Date();
        const diffMs = now.getTime() - date.getTime();
        const diffMins = Math.floor(diffMs / 60000);
        const diffHours = Math.floor(diffMs / 3600000);
        const diffDays = Math.floor(diffMs / 86400000);

        if (diffMins < 60) return `${diffMins}m ago`;
        if (diffHours < 24) return `${diffHours}h ago`;
        return `${diffDays}d ago`;
    };

    return (
        <div className="bg-gray-800 rounded-lg p-6 border border-gray-700">
            <h3 className="text-lg font-semibold text-white mb-6">AI Decision Feed</h3>

            <div className="space-y-4 max-h-96 overflow-y-auto">
                {data.map((decision, index) => (
                    <div
                        key={index}
                        className="bg-gray-700/50 rounded-lg p-4 border border-gray-600 hover:border-gray-500 transition-colors"
                    >
                        <div className="flex items-start justify-between mb-2">
                            <div className="flex-1">
                                <div className="flex items-center gap-2 mb-1">
                                    <span className="text-white font-medium text-sm">{decision.decision_type}</span>
                                    <span
                                        className={`px-2 py-0.5 rounded text-xs font-medium border ${getStatusColor(
                                            decision.status
                                        )}`}
                                    >
                                        {decision.status}
                                    </span>
                                </div>
                                <p className="text-gray-400 text-sm">{decision.entity}</p>
                            </div>
                            <span className="text-gray-500 text-xs whitespace-nowrap ml-2">
                                {formatTimestamp(decision.timestamp)}
                            </span>
                        </div>

                        <div className="space-y-2">
                            <p className="text-white text-sm">
                                <span className="text-gray-400">Action:</span> {decision.action}
                            </p>

                            <div className="flex items-center justify-between">
                                <div className="flex items-center gap-2">
                                    <span className="text-gray-400 text-xs">Confidence:</span>
                                    <span className={`font-medium text-sm ${getConfidenceColor(decision.confidence)}`}>
                                        {(decision.confidence * 100).toFixed(0)}%
                                    </span>
                                </div>
                                <span className="text-gray-400 text-xs">
                                    Impact: <span className="text-white">{decision.impact}</span>
                                </span>
                            </div>

                            <div className="w-full bg-gray-600 rounded-full h-1 overflow-hidden">
                                <div
                                    className={`h-full rounded-full transition-all ${decision.confidence >= 0.8
                                            ? 'bg-green-500'
                                            : decision.confidence >= 0.6
                                                ? 'bg-yellow-500'
                                                : 'bg-red-500'
                                        }`}
                                    style={{ width: `${decision.confidence * 100}%` }}
                                />
                            </div>
                        </div>
                    </div>
                ))}
            </div>
        </div>
    );
}
