"""
fintech_analytics.pipeline.dashboard
======================================
Serves the interactive analytics dashboard via a local HTTP server.
"""

from __future__ import annotations

import http.server
import json
import os
import tempfile
import threading
import webbrowser
from pathlib import Path
from typing import Optional

import duckdb
from rich.console import Console

console = Console()


def _build_dashboard_html(con: duckdb.DuckDBPyConnection, metrics: dict) -> str:
    """Build self-contained HTML dashboard from DuckDB data."""

    # Pull all data needed
    def q(sql):
        try:
            return con.execute(sql).df().to_dict(orient="records")
        except Exception:
            return []

    data = {
        "metrics":     metrics,
        "monthly":     q("SELECT strftime(cast(month as date), '%Y-%m') as month, * EXCLUDE(month) FROM analytics.payment_performance ORDER BY 1"),
        "rfm":         q("SELECT segment, count(*) as customers, round(avg(monetary),2) as avg_spend, round(sum(monetary),2) as total_spend FROM analytics.rfm GROUP BY 1 ORDER BY customers DESC"),
        "merchants":   q("SELECT * FROM analytics.merchant_scorecard ORDER BY fraud_rate_pct DESC LIMIT 20"),
        "cohort":      q("SELECT strftime(cast(cohort_month as date), '%Y-%m') as cohort, months_since_signup, retention_pct, cohort_size FROM analytics.cohort_retention WHERE months_since_signup <= 6 ORDER BY 1,2"),
    }

    data_js = f"const DATA = {json.dumps(data, default=str)};"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>fintech-analytics Dashboard</title>
<style>
:root{{--bg:#0b0f1e;--surf:#111827;--surf2:#1a2235;--border:#1e2d45;
  --accent:#00d4ff;--purple:#7c3aed;--green:#10b981;--red:#ef4444;
  --amber:#f59e0b;--text:#e2e8f0;--muted:#64748b;--font:system-ui,sans-serif;--mono:monospace}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--text);font-family:var(--font);font-size:14px}}
nav{{position:sticky;top:0;z-index:100;background:rgba(11,15,30,.97);border-bottom:1px solid var(--border);
  display:flex;align-items:center;padding:0 24px;height:52px;gap:0}}
.brand{{font-family:var(--mono);font-size:13px;font-weight:600;color:var(--accent);margin-right:32px}}
.tab{{padding:0 20px;height:52px;display:flex;align-items:center;font-size:12px;font-weight:500;
  color:var(--muted);cursor:pointer;border-bottom:2px solid transparent;transition:all .2s;
  text-transform:uppercase;letter-spacing:.05em}}
.tab:hover{{color:var(--text)}}.tab.active{{color:var(--accent);border-bottom-color:var(--accent)}}
.meta{{margin-left:auto;font-family:var(--mono);font-size:11px;color:var(--muted)}}
.page{{display:none;padding:28px;max-width:1400px;margin:0 auto;animation:fade .3s ease}}
.page.active{{display:block}}
@keyframes fade{{from{{opacity:0;transform:translateY(6px)}}to{{opacity:1;transform:none}}}}
.page-title{{font-size:22px;font-weight:600;margin-bottom:4px}}
.page-sub{{font-size:12px;color:var(--muted);font-family:var(--mono);margin-bottom:24px}}
.kpi-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:24px}}
.kpi{{background:var(--surf);border:1px solid var(--border);border-radius:8px;padding:18px 20px;position:relative;overflow:hidden}}
.kpi::before{{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--kc,var(--accent))}}
.kpi-label{{font-size:11px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-bottom:10px}}
.kpi-value{{font-family:var(--mono);font-size:26px;font-weight:600;color:var(--kc,var(--accent));line-height:1}}
.kpi-sub{{font-size:11px;color:var(--muted);margin-top:6px;font-family:var(--mono)}}
.grid2{{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-bottom:18px}}
.grid1{{display:grid;grid-template-columns:1fr;gap:18px;margin-bottom:18px}}
.card{{background:var(--surf);border:1px solid var(--border);border-radius:8px;padding:20px}}
.card-title{{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);
  margin-bottom:16px;font-family:var(--mono);display:flex;align-items:center;gap:8px}}
.card-title::before{{content:'';width:3px;height:12px;background:var(--accent);border-radius:2px}}
canvas{{display:block}}
.tw{{overflow-x:auto;max-height:380px;overflow-y:auto}}
table{{width:100%;border-collapse:collapse}}
thead th{{padding:9px 12px;text-align:left;font-size:11px;text-transform:uppercase;
  letter-spacing:.05em;color:var(--muted);background:var(--surf2);border-bottom:1px solid var(--border);
  position:sticky;top:0;white-space:nowrap}}
tbody tr{{border-bottom:1px solid var(--border);transition:background .15s}}
tbody tr:hover{{background:var(--surf2)}}td{{padding:9px 12px;font-size:13px;white-space:nowrap}}
.mono{{font-family:var(--mono)}}
.badge{{display:inline-flex;align-items:center;padding:2px 8px;border-radius:4px;
  font-size:11px;font-weight:600;font-family:var(--mono)}}
.b-crit{{background:rgba(239,68,68,.15);color:#f87171}}
.b-elev{{background:rgba(245,158,11,.15);color:#fbbf24}}
.b-std{{background:rgba(16,185,129,.15);color:#34d399}}
.seg-row{{display:flex;align-items:center;gap:10px;margin-bottom:8px}}
.seg-label{{width:160px;font-size:12px;flex-shrink:0}}
.seg-track{{flex:1;background:var(--surf2);border-radius:3px;height:22px;overflow:hidden}}
.seg-fill{{height:100%;border-radius:3px;display:flex;align-items:center;padding-left:8px;transition:width .8s ease}}
.seg-fill span{{font-size:11px;font-family:var(--mono);color:rgba(255,255,255,.8);font-weight:600}}
.seg-num{{width:50px;text-align:right;font-family:var(--mono);font-size:12px;color:var(--muted)}}
.cohort-cell{{text-align:center;font-family:var(--mono);font-size:12px;padding:7px 8px!important}}
::-webkit-scrollbar{{width:5px;height:5px}}
::-webkit-scrollbar-track{{background:var(--surf)}}
::-webkit-scrollbar-thumb{{background:var(--border);border-radius:3px}}
@media(max-width:900px){{.kpi-grid,.grid2{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<nav>
  <div class="brand">◈ FINTECH ANALYTICS</div>
  <div class="tab active" onclick="show('payments',this)">Payments</div>
  <div class="tab" onclick="show('customers',this)">Customers</div>
  <div class="tab" onclick="show('risk',this)">Risk</div>
  <div class="meta">fintech-analytics · open source</div>
</nav>

<div class="page active" id="page-payments">
  <div class="page-title">Payment Performance</div>
  <div class="page-sub">payment_performance · normalised · all currencies</div>
  <div class="kpi-grid" id="kpi1"></div>
  <div class="grid1"><div class="card"><div class="card-title">Monthly Volume</div><canvas id="c-vol" height="220"></canvas></div></div>
  <div class="grid2">
    <div class="card"><div class="card-title">Completion Rate Trend</div><canvas id="c-comp" height="200"></canvas></div>
    <div class="card"><div class="card-title">Fraud Rate Trend</div><canvas id="c-fraud" height="200"></canvas></div>
  </div>
</div>

<div class="page" id="page-customers">
  <div class="page-title">Customer Intelligence</div>
  <div class="page-sub">rfm_segmentation · cohort_retention</div>
  <div class="kpi-grid" id="kpi2"></div>
  <div class="grid1"><div class="card"><div class="card-title">RFM Segment Breakdown</div><div id="seg-bars"></div></div></div>
  <div class="grid1"><div class="card"><div class="card-title">Cohort Retention Matrix (M0–M6)</div><div class="tw"><table id="t-cohort"></table></div></div></div>
</div>

<div class="page" id="page-risk">
  <div class="page-title">Risk & Merchant Intelligence</div>
  <div class="page-sub">merchant_scorecard · risk bands</div>
  <div class="kpi-grid" id="kpi3"></div>
  <div class="card" style="margin-bottom:18px"><div class="card-title">Top 20 Merchants by Risk</div>
    <div class="tw"><table id="t-merchants"></table></div>
  </div>
</div>

<script>
{data_js}

const $=id=>document.getElementById(id);
const fmt=(n,d=0)=>n==null?'—':Number(n).toLocaleString('en-GB',{{minimumFractionDigits:d,maximumFractionDigits:d}});

function show(id,el){{
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  $('page-'+id).classList.add('active'); el.classList.add('active');
  setTimeout(drawAll,50);
}}

function kpi(el,items){{
  $(el).innerHTML=items.map(({label,value,sub,color})=>`
    <div class="kpi" style="--kc:${{color}}">
      <div class="kpi-label">${{label}}</div>
      <div class="kpi-value">${{value}}</div>
      ${{sub?`<div class="kpi-sub">${{sub}}</div>`:''}}
    </div>`).join('');
}}

const m=DATA.metrics;
kpi('kpi1',[
  {{label:'Total Volume',value:`${{m.total_volume?.toLocaleString('en-GB',{{maximumFractionDigits:0}})}}`,sub:'all currencies',color:'#00d4ff'}},
  {{label:'Transactions',value:fmt(m.total_transactions),sub:'total',color:'#7c3aed'}},
  {{label:'Completion',value:m.completion_pct+'%',sub:'target > 93%',color:'#10b981'}},
  {{label:'Fraud Rate',value:m.fraud_rate_pct+'%',sub:'flagged',color:'#ef4444'}},
]);
const rfmSum={{Champions:0,'Loyal Customers':0,'At Risk':0,Lost:0}};
DATA.rfm.forEach(r=>{{if(rfmSum[r.segment]!==undefined)rfmSum[r.segment]=r.customers;}});
kpi('kpi2',[
  {{label:'Customers',value:fmt(m.unique_customers),sub:'unique',color:'#00d4ff'}},
  {{label:'Champions',value:fmt(rfmSum['Champions']),sub:'top segment',color:'#7c3aed'}},
  {{label:'At Risk',value:fmt(rfmSum['At Risk']),sub:'need action',color:'#ef4444'}},
  {{label:'Merchants',value:fmt(m.unique_merchants),sub:'active',color:'#10b981'}},
]);
const critCount=DATA.merchants.filter(m=>m.risk_band==='critical').length;
kpi('kpi3',[
  {{label:'Total Merchants',value:fmt(m.unique_merchants),sub:'active',color:'#00d4ff'}},
  {{label:'Critical Risk',value:fmt(critCount),sub:'fraud > 5%',color:'#ef4444'}},
  {{label:'Date From',value:m.date_from||'—',sub:'earliest',color:'#7c3aed'}},
  {{label:'Date To',value:m.date_to||'—',sub:'latest',color:'#10b981'}},
]);

// Canvas chart helpers
const COLORS=['#00d4ff','#7c3aed','#10b981','#f59e0b','#ef4444','#f97316'];
const SEG_COLORS={{'Champions':'#00d4ff','Loyal Customers':'#7c3aed','Potential Loyalists':'#10b981','Recent Customers':'#3b82f6','Promising':'#06b6d4','Need Attention':'#f59e0b','About to Sleep':'#f97316','At Risk':'#ef4444','Cannot Lose Them':'#dc2626','Hibernating':'#6b7280','Lost':'#374151','Others':'#9ca3af'}};

function setupCanvas(id){{
  const c=$(id),p=c.parentElement;
  c.width=p.clientWidth-40; c.height=parseInt(c.getAttribute('height'));
  const ctx=c.getContext('2d'); ctx.clearRect(0,0,c.width,c.height);
  return{{ctx,W:c.width,H:c.height}};
}}
function barChart(id,labels,values,color,fmtY){{
  const{{ctx,W,H}}=setupCanvas(id);
  const pad={{l:65,r:20,t:15,b:35}},pW=W-pad.l-pad.r,pH=H-pad.t-pad.b;
  const maxV=Math.max(...values.filter(v=>v!=null))*1.15||1;
  const step=Math.ceil(labels.length/10);
  ctx.strokeStyle='#1e2d45';ctx.lineWidth=1;
  for(let i=0;i<=5;i++){{const y=pad.t+pH-(i/5)*pH;ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(pad.l+pW,y);ctx.stroke();ctx.fillStyle='#64748b';ctx.font='10px monospace';ctx.textAlign='right';ctx.fillText(fmtY?fmtY(maxV*i/5):(maxV*i/5).toFixed(0),pad.l-6,y+4);}}
  const bw=Math.min(pW/labels.length*0.7,50);
  values.forEach((v,i)=>{{if(v==null)return;const x=pad.l+i*(pW/labels.length)+pW/labels.length/2-bw/2,bh=(v/maxV)*pH,y=pad.t+pH-bh;ctx.fillStyle=color+'cc';ctx.fillRect(x,y,bw,bh);}});
  ctx.textAlign='center';ctx.fillStyle='#64748b';ctx.font='10px monospace';
  labels.forEach((l,i)=>{{if(i%step===0){{const x=pad.l+i*(pW/labels.length)+pW/labels.length/2;ctx.fillText(l,x,H-pad.b+14);}}}}); 
}}
function lineChart(id,labels,datasets){{
  const{{ctx,W,H}}=setupCanvas(id);
  const pad={{l:52,r:20,t:15,b:35}},pW=W-pad.l-pad.r,pH=H-pad.t-pad.b;
  datasets.forEach((ds,si)=>{{
    const vals=ds.data,yMin=Math.min(...vals)*0.995,yMax=Math.max(...vals)*1.005,range=yMax-yMin||1;
    if(si===0){{ctx.strokeStyle='#1e2d45';ctx.lineWidth=1;for(let i=0;i<=5;i++){{const y=pad.t+pH-(i/5)*pH;ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(pad.l+pW,y);ctx.stroke();ctx.fillStyle='#64748b';ctx.font='10px monospace';ctx.textAlign='right';ctx.fillText((yMin+(yMax-yMin)*i/5).toFixed(1)+'%',pad.l-4,y+4);}};}}
    ctx.beginPath();ctx.strokeStyle=ds.color;ctx.lineWidth=2;ctx.lineJoin='round';
    vals.forEach((v,i)=>{{const x=pad.l+(i/(labels.length-1))*pW,y=pad.t+pH-((v-yMin)/range)*pH;i===0?ctx.moveTo(x,y):ctx.lineTo(x,y);}});ctx.stroke();
    ctx.fillStyle=ds.color;
    vals.forEach((v,i)=>{{const x=pad.l+(i/(labels.length-1))*pW,y=pad.t+pH-((v-yMin)/range)*pH;ctx.beginPath();ctx.arc(x,y,3,0,Math.PI*2);ctx.fill();}});
  }});
  const step=Math.ceil(labels.length/10);
  ctx.textAlign='center';ctx.fillStyle='#64748b';ctx.font='10px monospace';
  labels.forEach((l,i)=>{{if(i%step===0){{const x=pad.l+(i/(labels.length-1))*pW;ctx.fillText(l,x,H-pad.b+14);}}}});
}}

function drawAll(){{
  const mv=DATA.monthly;
  if(!mv||!mv.length)return;
  const labels=mv.map(d=>d.month?.slice(2)||d.month);
  if($('c-vol')&&$('page-payments').classList.contains('active')){{
    const grouped={{}};mv.forEach(d=>{{if(!grouped[d.month])grouped[d.month]=0;grouped[d.month]+=(d.total_volume||0);}});
    const months=Object.keys(grouped).sort(),vols=months.map(m=>grouped[m]);
    barChart('c-vol',months.map(m=>m.slice(2)),vols,'#00d4ff',v=>'$'+(v/1000).toFixed(0)+'k');
    const cByM={{}};mv.forEach(d=>{{if(!cByM[d.month])cByM[d.month]=[];cByM[d.month].push(d.completion_pct||0);}});
    const fByM={{}};mv.forEach(d=>{{if(!fByM[d.month])fByM[d.month]=[];fByM[d.month].push(d.fraud_rate_pct||0);}});
    const mKeys=Object.keys(cByM).sort();
    lineChart('c-comp',mKeys.map(m=>m.slice(2)),[{{data:mKeys.map(m=>cByM[m].reduce((a,b)=>a+b,0)/cByM[m].length),color:'#10b981'}}]);
    lineChart('c-fraud',mKeys.map(m=>m.slice(2)),[{{data:mKeys.map(m=>fByM[m].reduce((a,b)=>a+b,0)/fByM[m].length),color:'#ef4444'}}]);
  }}
  if($('page-customers').classList.contains('active')){{
    const rfm=DATA.rfm;
    const maxC=Math.max(...rfm.map(r=>r.customers));
    $('seg-bars').innerHTML=rfm.map(r=>`
      <div class="seg-row">
        <div class="seg-label">${{r.segment}}</div>
        <div class="seg-track"><div class="seg-fill" style="width:${{(r.customers/maxC*100)}}%;background:${{SEG_COLORS[r.segment]||'#475569'}}33;border-left:3px solid ${{SEG_COLORS[r.segment]||'#475569'}}">
          <span style="color:${{SEG_COLORS[r.segment]||'#475569'}}">${{r.segment}} · avg ${{fmt(r.avg_spend)}}</span></div></div>
        <div class="seg-num">${{r.customers}}</div>
      </div>`).join('');
    const cohorts=[...new Set(DATA.cohort.map(d=>d.cohort))].slice(-8);
    const months=[0,1,2,3,4,5,6];
    const mp={{}};DATA.cohort.forEach(d=>{{if(!mp[d.cohort])mp[d.cohort]={{}};mp[d.cohort][d.months_since_signup]=d.retention_pct;}});
    function rc(v){{if(v==null)return'#0f172a';if(v>=95)return'rgba(16,185,129,.35)';if(v>=90)return'rgba(0,212,255,.2)';if(v>=85)return'rgba(245,158,11,.2)';return'rgba(239,68,68,.2)';}}
    $('t-cohort').innerHTML=`<thead><tr><th>Cohort</th><th>Size</th>${{months.map(m=>`<th>M${{m}}</th>`).join('')}}</tr></thead>`+`<tbody>${{cohorts.map(c=>{{const size=DATA.cohort.find(d=>d.cohort===c)?.cohort_size||'—';return`<tr><td class="mono" style="color:#00d4ff">${{c}}</td><td class="mono" style="color:#64748b">${{size}}</td>${{months.map(m=>{{const v=mp[c]?.[m];return`<td class="cohort-cell" style="background:${{rc(v)}}">${{v!=null?v+'%':'—'}}</td>`;}}).join('')}}</tr>`;}}).join('')}}</tbody>`;
  }}
  if($('page-risk').classList.contains('active')){{
    $('t-merchants').innerHTML=`<thead><tr><th>Merchant</th><th>Category</th><th>Transactions</th><th>Volume</th><th>Fraud %</th><th>Completion %</th><th>Band</th></tr></thead>`+`<tbody>${{DATA.merchants.map(d=>`<tr><td style="font-weight:500">${{d.merchant_name||'Unknown'}}</td><td style="color:#94a3b8">${{d.merchant_category||'—'}}</td><td class="mono">${{fmt(d.total_transactions)}}</td><td class="mono" style="color:#94a3b8">${{fmt(d.total_volume)}}</td><td class="mono" style="color:${{d.fraud_rate_pct>4?'#f87171':d.fraud_rate_pct>2?'#fbbf24':'#34d399'}}">${{d.fraud_rate_pct||0}}%</td><td class="mono">${{d.completion_rate_pct||0}}%</td><td><span class="badge b-${{d.risk_band==='critical'?'crit':d.risk_band==='elevated'?'elev':'std'}}">${{d.risk_band}}</span></td></tr>`).join('')}}</tbody>`;
  }}
}}
drawAll();
window.addEventListener('resize',()=>setTimeout(drawAll,100));
</script>
</body>
</html>"""
    return html


def serve_dashboard(
    con: duckdb.DuckDBPyConnection,
    metrics: dict,
    port: int = 8888,
    open_browser: bool = True,
):
    """Serve the dashboard as a local HTTP server."""
    html = _build_dashboard_html(con, metrics)

    # Write to temp file
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".html", delete=False, encoding="utf-8"
    )
    tmp.write(html)
    tmp.close()
    tmp_path = Path(tmp.name)

    # Simple one-file HTTP server
    import http.server
    import socketserver
    import os

    os.chdir(tmp_path.parent)

    class Handler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            self.path = f"/{tmp_path.name}"
            return super().do_GET()

        def log_message(self, *args):
            pass  # suppress request logs

    url = f"http://localhost:{port}"
    console.print(f"\n[bold cyan]Dashboard running at[/bold cyan] {url}")
    console.print("[dim]Press Ctrl-C to stop[/dim]\n")

    if open_browser:
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()

    with socketserver.TCPServer(("", port), Handler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            console.print("\n[dim]Dashboard stopped[/dim]")
