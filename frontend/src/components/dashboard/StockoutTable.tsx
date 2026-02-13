// Stockout risks table component

import type { StockoutRisk } from '@/types/kpi';

interface StockoutTableProps {
    data: StockoutRisk[];
}

export default function StockoutTable({ data }: StockoutTableProps) {
    if (!data || data.length === 0) {
        return (
            <div className="bg-gray-800 rounded-lg p-6 border border-gray-700">
                <h3 className="text-lg font-semibold text-white mb-4">Inventory Movement</h3>
                <p className="text-gray-500">No data available</p>
            </div>
        );
    }

    const topProducts = data.slice(0, 10);

    const getCategoryColor = (category: string) => {
        switch (category) {
            case 'Fast Moving':
                return 'bg-green-500/20 text-green-400 border-green-500/30';
            case 'Medium Moving':
                return 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30';
            case 'Slow Moving':
                return 'bg-red-500/20 text-red-400 border-red-500/30';
            default:
                return 'bg-gray-500/20 text-gray-400 border-gray-500/30';
        }
    };

    return (
        <div className="bg-gray-800 rounded-lg p-6 border border-gray-700">
            <h3 className="text-lg font-semibold text-white mb-6">Inventory Movement Analysis</h3>

            <div className="overflow-x-auto">
                <table className="w-full">
                    <thead>
                        <tr className="border-b border-gray-700">
                            <th className="text-left text-gray-400 text-sm font-medium pb-3">Product</th>
                            <th className="text-left text-gray-400 text-sm font-medium pb-3">Category</th>
                            <th className="text-right text-gray-400 text-sm font-medium pb-3">Avg Daily Sales</th>
                            <th className="text-right text-gray-400 text-sm font-medium pb-3">Movement</th>
                            <th className="text-right text-gray-400 text-sm font-medium pb-3">Revenue</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-700">
                        {topProducts.map((product, index) => (
                            <tr key={index} className="hover:bg-gray-700/50 transition-colors">
                                <td className="py-3">
                                    <div>
                                        <p className="text-white font-medium text-sm">{product.product_name}</p>
                                        <p className="text-gray-500 text-xs">{product.product_id}</p>
                                    </div>
                                </td>
                                <td className="py-3">
                                    <span className="text-gray-400 text-sm">{product.category}</span>
                                </td>
                                <td className="py-3 text-right">
                                    <span className="text-white font-medium text-sm">
                                        {product.avg_daily_sales.toFixed(1)}
                                    </span>
                                </td>
                                <td className="py-3 text-right">
                                    <span
                                        className={`inline-block px-2 py-1 rounded text-xs font-medium border ${getCategoryColor(
                                            product.movement_category
                                        )}`}
                                    >
                                        {product.movement_category}
                                    </span>
                                </td>
                                <td className="py-3 text-right">
                                    <span className="text-white font-medium text-sm">
                                        ₹{product.total_revenue.toLocaleString('en-IN', { maximumFractionDigits: 0 })}
                                    </span>
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
