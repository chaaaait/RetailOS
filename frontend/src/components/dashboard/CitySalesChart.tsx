// City sales chart component displaying regional performance

import type { CitySales } from '@/types/kpi';

interface CitySalesChartProps {
    data: CitySales[];
}

export default function CitySalesChart({ data }: CitySalesChartProps) {
    if (!data || data.length === 0) {
        return (
            <div className="bg-gray-800 rounded-lg p-6 border border-gray-700">
                <h3 className="text-lg font-semibold text-white mb-4">City Sales Performance</h3>
                <p className="text-gray-500">No data available</p>
            </div>
        );
    }

    const maxRevenue = Math.max(...data.map((d) => d.total_revenue));
    const topCities = data.slice(0, 8);

    return (
        <div className="bg-gray-800 rounded-lg p-6 border border-gray-700">
            <h3 className="text-lg font-semibold text-white mb-6">City Sales Performance</h3>

            <div className="space-y-4">
                {topCities.map((city, index) => {
                    const percentage = (city.total_revenue / maxRevenue) * 100;

                    return (
                        <div key={index} className="space-y-2">
                            <div className="flex items-center justify-between">
                                <div className="flex items-center gap-3">
                                    <span className="text-gray-500 text-sm font-medium w-6">
                                        #{index + 1}
                                    </span>
                                    <div>
                                        <p className="text-white font-medium">{city.city}</p>
                                        <p className="text-gray-500 text-xs">{city.region}</p>
                                    </div>
                                </div>
                                <div className="text-right">
                                    <p className="text-white font-semibold">
                                        ₹{city.total_revenue.toLocaleString('en-IN', { maximumFractionDigits: 0 })}
                                    </p>
                                    <p className="text-gray-500 text-xs">
                                        {city.revenue_share_pct.toFixed(1)}% share
                                    </p>
                                </div>
                            </div>
                            <div className="w-full bg-gray-700 rounded-full h-2 overflow-hidden">
                                <div
                                    className="h-full bg-gradient-to-r from-blue-500 to-cyan-500 rounded-full transition-all"
                                    style={{ width: `${percentage}%` }}
                                />
                            </div>
                            <div className="flex items-center justify-between text-xs text-gray-500">
                                <span>{city.active_stores} stores</span>
                                <span>{city.transaction_count.toLocaleString('en-IN')} transactions</span>
                            </div>
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
