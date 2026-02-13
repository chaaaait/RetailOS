// Loading skeleton component for dashboard

export function SkeletonCard() {
    return (
        <div className="bg-gray-800 rounded-lg p-6 border border-gray-700 animate-pulse">
            <div className="h-4 bg-gray-700 rounded w-1/3 mb-4"></div>
            <div className="h-8 bg-gray-700 rounded w-1/2 mb-2"></div>
            <div className="h-3 bg-gray-700 rounded w-1/4"></div>
        </div>
    );
}

export function SkeletonChart() {
    return (
        <div className="bg-gray-800 rounded-lg p-6 border border-gray-700 animate-pulse">
            <div className="h-5 bg-gray-700 rounded w-1/3 mb-6"></div>
            <div className="space-y-4">
                {[...Array(5)].map((_, i) => (
                    <div key={i} className="space-y-2">
                        <div className="flex items-center justify-between">
                            <div className="h-4 bg-gray-700 rounded w-1/4"></div>
                            <div className="h-4 bg-gray-700 rounded w-1/6"></div>
                        </div>
                        <div className="h-2 bg-gray-700 rounded w-full"></div>
                    </div>
                ))}
            </div>
        </div>
    );
}

export function SkeletonTable() {
    return (
        <div className="bg-gray-800 rounded-lg p-6 border border-gray-700 animate-pulse">
            <div className="h-5 bg-gray-700 rounded w-1/3 mb-6"></div>
            <div className="space-y-3">
                {[...Array(6)].map((_, i) => (
                    <div key={i} className="flex items-center justify-between">
                        <div className="h-4 bg-gray-700 rounded w-1/3"></div>
                        <div className="h-4 bg-gray-700 rounded w-1/6"></div>
                        <div className="h-4 bg-gray-700 rounded w-1/6"></div>
                    </div>
                ))}
            </div>
        </div>
    );
}
