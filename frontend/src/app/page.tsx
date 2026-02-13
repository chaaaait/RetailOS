"use client"

import { useEffect, useState } from "react"

export default function Home() {
  const [data, setData] = useState<any[]>([])

  useEffect(() => {
    fetch("http://127.0.0.1:8000/api/kpi/daily-revenue")
      .then(res => res.json())
      .then(setData)
  }, [])

  return (
    <main className="p-10">
      <h1 className="text-3xl font-bold mb-6">RetailOS Dashboard</h1>

      <h2 className="text-xl font-semibold mb-2">Daily Revenue</h2>
      <pre className="bg-gray-100 p-4 rounded">
        {JSON.stringify(data, null, 2)}
      </pre>
    </main>
  )
}
