// Reusable StatCard component for displaying KPI metrics

interface StatCardProps {
    title: string;
    value: string | number;
    subtitle?: string;
    trend?: {
        value: number;
        isPositive: boolean;
    };
    icon?: React.ReactNode;
}

export default function StatCard({ title, value, subtitle, trend, icon }: StatCardProps) {
    return (
        <div className="bg-gray-800 rounded-lg p-6 border border-gray-700 hover:border-gray-600 transition-colors">
            <div className="flex items-start justify-between">
                <div className="flex-1">
                    <p className="text-gray-400 text-sm font-medium mb-1">{title}</p>
                    <p className="text-3xl font-bold text-white mb-2">{value}</p>
                    {subtitle && <p className="text-gray-500 text-sm">{subtitle}</p>}
                    {trend && (
                        <div className="flex items-center mt-2">
                            <span
                                className={`text-sm font-medium ${trend.isPositive ? 'text-green-400' : 'text-red-400'
                                    }`}
                            >
                                {trend.isPositive ? '↑' : '↓'} {Math.abs(trend.value)}%
                            </span>
                            <span className="text-gray-500 text-sm ml-2">vs last period</span>
                        </div>
                    )}
                </div>
                {icon && <div className="text-gray-600 ml-4">{icon}</div>}
            </div>
        </div>
    );
}
