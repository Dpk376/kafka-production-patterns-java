import { useState } from 'react';
import { generateTraffic, sendPoisonPill } from './api';
import { Activity, Skull, Terminal, Play, Loader2, CheckCircle2, AlertCircle } from 'lucide-react';
import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

type LogEntry = {
  id: string;
  time: Date;
  message: string;
  type: 'success' | 'error' | 'info';
};

function App() {
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [isLoadingTraffic, setIsLoadingTraffic] = useState(false);
  const [isLoadingPoison, setIsLoadingPoison] = useState(false);
  const [trafficCount, setTrafficCount] = useState<number>(10);

  const addLog = (message: string, type: 'success' | 'error' | 'info' = 'info') => {
    setLogs(prev => [
      { id: Math.random().toString(36).substring(7), time: new Date(), message, type },
      ...prev
    ].slice(0, 100));
  };

  const handleGenerateTraffic = async () => {
    setIsLoadingTraffic(true);
    addLog(`Initiating generation of ${trafficCount} events...`, 'info');
    try {
      const result = await generateTraffic(trafficCount);
      addLog(`Success: ${result}`, 'success');
    } catch (err: any) {
      addLog(`Failed: ${err.message || 'Network error (CORS?)'}`, 'error');
    } finally {
      setIsLoadingTraffic(false);
    }
  };

  const handleSendPoisonPill = async () => {
    setIsLoadingPoison(true);
    addLog(`Sending poison pill event...`, 'info');
    try {
      const result = await sendPoisonPill();
      addLog(`Success: ${result}`, 'success');
    } catch (err: any) {
      addLog(`Failed: ${err.message || 'Network error (CORS?)'}`, 'error');
    } finally {
      setIsLoadingPoison(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-950 text-slate-200 font-sans selection:bg-indigo-500/30">
      <div className="max-w-5xl mx-auto px-6 py-12 flex flex-col gap-8">
        
        {/* Header */}
        <header className="flex flex-col gap-2">
          <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-indigo-500/10 text-indigo-400 text-sm font-medium w-fit border border-indigo-500/20 shadow-inner">
            <Activity className="w-4 h-4 animate-pulse" />
            System Live
          </div>
          <h1 className="text-4xl font-bold tracking-tight text-slate-100 mt-2">
            Kafka Production Patterns
          </h1>
          <p className="text-slate-400 text-lg max-w-2xl">
            Control panel for load generation. Trigger distributed events to observe Debezium outbox, idempotent consumers, and Dead Letter Queue handling.
          </p>
        </header>

        <div className="grid grid-cols-1 lg:grid-cols-12 gap-8">
          
          {/* Controls Area */}
          <div className="lg:col-span-5 flex flex-col gap-6">
            
            {/* Generate Traffic Card */}
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl relative overflow-hidden group">
              <div className="absolute inset-0 bg-gradient-to-br from-indigo-500/5 to-purple-500/5 opacity-0 group-hover:opacity-100 transition-opacity" />
              <div className="relative z-10 flex flex-col gap-5">
                <div className="flex items-center gap-3">
                  <div className="p-3 bg-indigo-500/20 rounded-xl text-indigo-400">
                    <Play className="w-6 h-6" />
                  </div>
                  <div>
                    <h3 className="text-xl font-semibold text-slate-100">Traffic Generator</h3>
                    <p className="text-slate-400 text-sm">Simulate burst order events.</p>
                  </div>
                </div>

                <div className="flex flex-col gap-2">
                  <label className="text-sm font-medium text-slate-300">Number of Events</label>
                  <input 
                    type="range" 
                    min="1" 
                    max="100" 
                    value={trafficCount} 
                    onChange={(e) => setTrafficCount(parseInt(e.target.value))}
                    className="w-full accent-indigo-500"
                  />
                  <div className="flex justify-between text-xs text-slate-500 font-medium">
                    <span>1</span>
                    <span className="text-indigo-400">{trafficCount} events</span>
                    <span>100</span>
                  </div>
                </div>

                <button 
                  onClick={handleGenerateTraffic}
                  disabled={isLoadingTraffic}
                  className="mt-2 flex items-center justify-center gap-2 w-full bg-indigo-600 hover:bg-indigo-500 text-white font-semibold py-3 px-4 rounded-xl transition-all disabled:opacity-50 disabled:cursor-not-allowed shadow-[0_0_15px_rgba(79,70,229,0.3)] hover:shadow-[0_0_25px_rgba(79,70,229,0.5)] active:scale-[0.98]"
                >
                  {isLoadingTraffic ? <Loader2 className="w-5 h-5 animate-spin" /> : 'Generate Now'}
                </button>
              </div>
            </div>

            {/* Poison Pill Card */}
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl relative overflow-hidden group">
              <div className="absolute inset-0 bg-gradient-to-br from-rose-500/5 to-orange-500/5 opacity-0 group-hover:opacity-100 transition-opacity" />
              <div className="relative z-10 flex flex-col gap-5">
                <div className="flex items-center gap-3">
                  <div className="p-3 bg-rose-500/20 rounded-xl text-rose-400">
                    <Skull className="w-6 h-6" />
                  </div>
                  <div>
                    <h3 className="text-xl font-semibold text-slate-100">Inject Poison Pill</h3>
                    <p className="text-slate-400 text-sm">Trigger Dead Letter Queue routing.</p>
                  </div>
                </div>

                <p className="text-sm text-slate-400">
                  Injects a deliberately malformed message into the system to test DLQ configuration and resilience.
                </p>

                <button 
                  onClick={handleSendPoisonPill}
                  disabled={isLoadingPoison}
                  className="mt-2 flex items-center justify-center gap-2 w-full bg-slate-800 hover:bg-rose-950/50 text-rose-400 hover:text-rose-300 font-semibold py-3 px-4 rounded-xl border border-rose-500/20 hover:border-rose-500/50 transition-all disabled:opacity-50 disabled:cursor-not-allowed active:scale-[0.98]"
                >
                  {isLoadingPoison ? <Loader2 className="w-5 h-5 animate-spin" /> : 'Send Poison Pill'}
                </button>
              </div>
            </div>
            
          </div>

          {/* Terminal / Logs Area */}
          <div className="lg:col-span-7">
            <div className="bg-[#0D1117] border border-slate-800 rounded-2xl flex flex-col h-full shadow-2xl overflow-hidden">
              <div className="bg-slate-900/50 px-4 py-3 border-b border-slate-800 flex items-center justify-between">
                <div className="flex items-center gap-2 text-slate-400 text-sm font-medium">
                  <Terminal className="w-4 h-4" />
                  Execution Logs
                </div>
                <div className="flex gap-1.5">
                  <div className="w-3 h-3 rounded-full bg-rose-500/20 border border-rose-500/50" />
                  <div className="w-3 h-3 rounded-full bg-amber-500/20 border border-amber-500/50" />
                  <div className="w-3 h-3 rounded-full bg-emerald-500/20 border border-emerald-500/50" />
                </div>
              </div>
              
              <div className="p-4 flex-1 flex flex-col gap-2 overflow-y-auto max-h-[500px] font-mono text-sm">
                {logs.length === 0 ? (
                  <div className="flex flex-col items-center justify-center h-full text-slate-600 gap-2">
                    <Terminal className="w-8 h-8 opacity-50" />
                    <p>No activity yet. Trigger an action.</p>
                  </div>
                ) : (
                  logs.map((log) => (
                    <div 
                      key={log.id} 
                      className="flex items-start gap-3 py-1 animate-in fade-in slide-in-from-bottom-2 duration-300"
                    >
                      <span className="text-slate-600 shrink-0 select-none">
                        {log.time.toLocaleTimeString([], { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' })}
                      </span>
                      {log.type === 'success' && <CheckCircle2 className="w-4 h-4 text-emerald-400 mt-0.5 shrink-0" />}
                      {log.type === 'error' && <AlertCircle className="w-4 h-4 text-rose-400 mt-0.5 shrink-0" />}
                      {log.type === 'info' && <span className="text-indigo-400 mt-0.5 shrink-0">→</span>}
                      
                      <span className={cn(
                        "break-all",
                        log.type === 'success' && "text-emerald-300",
                        log.type === 'error' && "text-rose-300",
                        log.type === 'info' && "text-slate-300"
                      )}>
                        {log.message}
                      </span>
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>

        </div>
      </div>
    </div>
  );
}

export default App;
