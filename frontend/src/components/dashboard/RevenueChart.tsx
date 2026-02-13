// Revenue chart component displaying daily revenue trends

import type { DailyRevenue } from '@/types/kpi';

interface RevenueChartProps {
    data: DailyRevenue[];
}

export default function RevenueChart({ data }: RevenueChartProps) {
    if (!data || data.length === 0) {
        return (
            <div className="bg-gray-800 rounded-lg p-6 border border-gray-700">
                <h3 className="text-lg font-semibold text-white mb-4">Daily Revenue Trend</h3>
                <p className="text-gray-500">No data available</p>
            </div>
        );
    }

    const maxRevenue = Math.max(...data.map((d) => d.revenue));
    const totalRevenue = data.reduce((sum, d) => sum + d.revenue, 0);
    const avgRevenue = totalRevenue / data.length;

    return (
        <div className="bg-gray-800 rounded-lg p-6 border border-gray-700">
            <div className="flex items-center justify-between mb-6">
                <h3 className="text-lg font-semibold text-white">Daily Revenue Trend</h3>
                <div className="text-right">
                    <p className="text-sm text-gray-400">Total Revenue</p>
                    <p className="text-xl font-bold text-white">
                        ₹{totalRevenue.toLocaleString('en-IN', { maximumFractionDigits: 0 })}
                    </p>
                </div>
            </div>

            <div className="space-y-3">
                {data.slice(0, 10).map((item, index) => {
                    const percentage = (item.revenue / maxRevenue) * 100;
                    const isAboveAvg = item.revenue > avgRevenue;

                    return (
                        <div key={index} className="space-y-1">
                            <div className="flex items-center justify-between text-sm">
                                <span className="text-gray-400">
                                    {new Date(item.date).toLocaleDateString('en-IN', {
                                        month: 'short',
                                        day: 'numeric',
                                    })}
                                </span>
                                <span className="text-white font-medium">
                                    ₹{item.revenue.toLocaleString('en-IN', { maximumFractionDigits: 0 })}
                                </span>
                            </div>
                            <div className="w-full bg-gray-700 rounded-full h-2 overflow-hidden">
                                <div
                                    className={`h-full rounded-full transition-all ${isAboveAvg ? 'bg-green-500' : 'bg-blue-500'
                                        }`}
                                    style={{ width: `${percentage}%` }}
                                />
                            </div>
                        </div>
                    );
                })}
            </div>

            <div className="mt-4 pt-4 border-t border-gray-700">
                <div className="flex items-center justify-between text-sm">
                    <span className="text-gray-400">Average Daily Revenue</span>
                    <span className="text-white font-medium">
                        ₹{avgRevenue.toLocaleString('en-IN', { maximumFractionDigits: 0 })}
                    </span>
                </div>
            </div>
        </div>
    );
}
