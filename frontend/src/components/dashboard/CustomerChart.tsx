// Customer distribution chart component

import type { CustomerDistribution } from '@/types/kpi';

interface CustomerChartProps {
    data: CustomerDistribution[];
}

export default function CustomerChart({ data }: CustomerChartProps) {
    if (!data || data.length === 0) {
        return (
            <div className="bg-gray-800 rounded-lg p-6 border border-gray-700">
                <h3 className="text-lg font-semibold text-white mb-4">Customer Distribution</h3>
                <p className="text-gray-500">No data available</p>
            </div>
        );
    }

    const totalCustomers = data.reduce((sum, d) => sum + d.customer_count, 0);
    const totalRevenue = data.reduce((sum, d) => sum + d.total_revenue, 0);

    const getTierColor = (tier: string) => {
        switch (tier) {
            case 'Metro':
                return 'from-purple-500 to-pink-500';
            case 'Tier-1':
                return 'from-blue-500 to-cyan-500';
            case 'Tier-2':
                return 'from-green-500 to-emerald-500';
            default:
                return 'from-gray-500 to-gray-600';
        }
    };

    const getSegmentColor = (segment: string) => {
        switch (segment) {
            case 'Premium':
                return 'text-purple-400';
            case 'High Value':
                return 'text-blue-400';
            case 'Medium Value':
                return 'text-green-400';
            case 'Low Value':
                return 'text-gray-400';
            default:
                return 'text-gray-500';
        }
    };

    return (
        <div className="bg-gray-800 rounded-lg p-6 border border-gray-700">
            <div className="flex items-center justify-between mb-6">
                <h3 className="text-lg font-semibold text-white">Customer Distribution</h3>
                <div className="text-right">
                    <p className="text-sm text-gray-400">Total Customers</p>
                    <p className="text-xl font-bold text-white">
                        {totalCustomers.toLocaleString('en-IN')}
                    </p>
                </div>
            </div>

            <div className="space-y-4">
                {data.slice(0, 6).map((item, index) => {
                    const customerPercentage = (item.customer_count / totalCustomers) * 100;
                    const revenuePercentage = (item.total_revenue / totalRevenue) * 100;

                    return (
                        <div key={index} className="space-y-2">
                            <div className="flex items-center justify-between">
                                <div>
                                    <p className="text-white font-medium">{item.city}</p>
                                    <div className="flex items-center gap-2 mt-1">
                                        <span className="text-xs text-gray-500">{item.city_tier}</span>
                                        <span className="text-xs text-gray-600">•</span>
                                        <span className={`text-xs font-medium ${getSegmentColor(item.value_segment)}`}>
                                            {item.value_segment}
                                        </span>
                                    </div>
                                </div>
                                <div className="text-right">
                                    <p className="text-white font-semibold">
                                        {item.customer_count.toLocaleString('en-IN')}
                                    </p>
                                    <p className="text-gray-500 text-xs">{customerPercentage.toFixed(1)}%</p>
                                </div>
                            </div>

                            <div className="space-y-1">
                                <div className="w-full bg-gray-700 rounded-full h-2 overflow-hidden">
                                    <div
                                        className={`h-full bg-gradient-to-r ${getTierColor(item.city_tier)} rounded-full transition-all`}
                                        style={{ width: `${customerPercentage}%` }}
                                    />
                                </div>
                                <div className="flex items-center justify-between text-xs text-gray-500">
                                    <span>
                                        ₹{item.avg_clv.toLocaleString('en-IN', { maximumFractionDigits: 0 })} avg CLV
                                    </span>
                                    <span>{revenuePercentage.toFixed(1)}% revenue share</span>
                                </div>
                            </div>
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
