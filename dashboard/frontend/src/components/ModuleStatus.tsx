const modules = [
  'Market Data', 'Scheduler', 'Signal Engine', 'Risk Manager',
  'Execution', 'Portfolio', 'Guardian', 'Monitor',
];

export function ModuleStatus() {
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-xs uppercase tracking-wider text-gray-400 mb-2">Module Status</h2>
      <div className="grid grid-cols-2 gap-2">
        {modules.map(m => (
          <div key={m} className="flex items-center gap-2 text-sm">
            <span className="w-2 h-2 rounded-full bg-green-400" />
            {m}
          </div>
        ))}
      </div>
    </div>
  );
}
