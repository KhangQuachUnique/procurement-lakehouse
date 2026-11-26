"""Generate the local PBIP from inspected sources; source Parquet files remain untouched."""
from __future__ import annotations

import json
import uuid
from pathlib import Path
from profile_sources import PROJECT, WORKSPACE, FKS, discover, save
import pyarrow.parquet as pq

MODEL = PROJECT / 'ProcurementDashboard.SemanticModel'
REPORT = PROJECT / 'ProcurementDashboard.Report'
BASE = 'https://developer.microsoft.com/json-schemas/fabric/'
NAVY, TEAL, INK, MUTED, BG = '#142D4E', '#087F8C', '#172B4D', '#64748B', '#F7F9FC'
STEEL, OCHRE, SAGE, NEUTRAL = '#6487AD', '#A87945', '#86B8A9', '#ADB5BD'
PALETTE = [NAVY, TEAL, STEEL, OCHRE, SAGE, NEUTRAL]
SERIES_COLORS = {
    'Trúng thầu · tỷ VND': TEAL, 'Phân bổ · tỷ VND': TEAL,
    'Độ phủ giá trúng': TEAL, 'Độ phủ mẫu mô tả': NAVY,
    'Tỷ trọng giá kế hoạch VND': NAVY, 'Tỷ trọng gói VND': STEEL,
    'Mẫu mô tả': TEAL, 'Mẫu thời gian': TEAL,
    'Mẫu ngoài so sánh': NEUTRAL, 'Mẫu thời gian bị loại': NEUTRAL,
    'Đối sánh tập chọn': OCHRE, 'Tỷ trọng Top 10 người nhận': TEAL,
}
MEASURES = {}
MONEY = '#,0.00;(#,0.00);0.00'
COUNT = '#,0'
PCT = '0.00%;(0.00%);0.00%'


def uid(value):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, 'procurement-dashboard/'+value))


def text(path, value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(value.rstrip()+'\n', encoding='utf-8')


def measure(name, dax, fmt=COUNT, folder='Tổng quan', desc=''):
    MEASURES[name] = {'dax':dax,'format':fmt,'folder':folder,'description':desc or name}
    return name


def vn_sum(name, fact, col, fmt=COUNT, folder='Tổng quan', sample=None):
    dax=f'SUM({fact}[{col}])'
    if sample:
        dax=f'IF(SUM({fact}[{sample}]) > 0, {dax})'
    dax=f'CALCULATE({dax}, KEEPFILTERS(dim_currency[currency_code] = "VND"))'
    return measure(name,dax,fmt,folder,f'{fact}[{col}]; chỉ VND, không quy đổi ngoại tệ.' + (f' Chỉ hiển thị khi {sample}>0.' if sample else ''))


def define_measures():
    from gold_v3_dashboard import define_measures as define_v3
    define_v3()


EXTRA = {
 'dim_date': [('year_month_order','int64'),('year_quarter_order','int64'),('month_start','dateTime')],
 'dim_geography': [('province_display','string')],
 'dim_party': [('party_display','string')],
 'dim_duration_metric': [('metric_label','string')],
 'dim_progress_status': [('status_label','string')],
 'fact_supplier_daily': [('allocation_label','string')],
}
LABELS={'close_to_open_hours':'Đóng thầu → mở thầu','open_to_award_days':'Mở thầu → kết quả',
 'planned_to_actual_delay_days':'Độ trễ so với kế hoạch','plan_to_notice_days':'Kế hoạch → thông báo',
 'notice_to_close_hours':'Thông báo → đóng thầu','close_to_first_award_days':'Đóng thầu → kết quả đầu',
 'close_to_complete_award_days':'Đóng thầu → kết quả đầy đủ'}


def model(files):
    dates=[v for part in files['dim_date'] for v in pq.read_table(part,columns=['date_key'])['date_key'].to_pylist()]
    calendar_ok=bool(dates) and None not in dates and len(dates)==len(set(dates)) and (max(dates)-min(dates)).days+1==len(dates)
    save(PROJECT/'ProcurementDashboard.pbip',{'$schema':BASE+'pbip/pbipProperties/1.0.0/schema.json','version':'1.0','artifacts':[{'report':{'path':REPORT.name}}],'settings':{'enableAutoRecovery':True}})
    save(MODEL/'definition.pbism',{'$schema':BASE+'item/semanticModel/definitionProperties/1.0.0/schema.json','version':'4.2','settings':{'qnaEnabled':False}})
    text(MODEL/'definition/database.tmdl','database\n\tcompatibilityLevel: 1601')
    text(MODEL/'definition/model.tmdl','''model Model
	culture: vi-VN
	defaultPowerBIDataSourceVersion: powerBI_V3
	sourceQueryCulture: en-US
	dataAccessOptions
		legacyRedirects
	annotation PBI_TimeIntelligenceEnabled = 0
''')
    expr=f'''expression DataRoot = "{(WORKSPACE/'PowerBI/export/parquet').as_posix()}" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]
	lineageTag: {uid('DataRoot')}

expression ReadParquetTable =
		(tableName as text, expectedColumns as list) as table =>
		let
		    Root = Text.TrimEnd(Text.Replace(DataRoot, "\\", "/"), "/") & "/",
		    Files = Folder.Files(DataRoot),
		    Selected = Table.SelectRows(Files, each Text.Lower([Extension]) = ".parquet" and
		        ([Name] = tableName & ".parquet" or List.Contains(Text.Split(Text.Range(Text.Replace([Folder Path], "\\", "/"), Text.Length(Root)), "/"), tableName))),
		    Ordered = Table.Sort(Selected, {{{{"Folder Path", Order.Ascending}}, {{"Name", Order.Ascending}}}}),
		    Parts = if Table.RowCount(Ordered) = 0 then error Error.Record("MissingTable", tableName, DataRoot)
		        else List.Transform(Table.ToRecords(Ordered), each Parquet.Document(File.Contents([Folder Path] & [Name]))),
		    Checked = List.Transform(Parts, each if List.Sort(Table.ColumnNames(_)) <> List.Sort(expectedColumns)
		        then error Error.Record("SchemaMismatch", tableName, Table.ColumnNames(_)) else _),
		    Combined = Table.Combine(Checked)
		in Combined
	lineageTag: {uid('ReadParquetTable')}
'''
    text(MODEL/'definition/expressions.tmdl',expr)
    definitions={}
    for name,paths in files.items():
        schema=pq.read_schema(paths[0])
        fields=[]
        mtypes=[]
        for c in schema:
            typ=str(c.type)
            data_type='string' if typ=='string' else 'boolean' if typ=='bool' else 'dateTime' if typ.startswith('date') else 'int64' if typ.startswith('int') else 'double'
            fields.append((c.name,data_type))
            mt='type text' if data_type=='string' else 'type logical' if data_type=='boolean' else 'type date' if data_type=='dateTime' else 'Int64.Type' if data_type=='int64' else 'type number'
            mtypes.append('{"'+c.name+'", '+mt+'}')
        body=f'table {name}\n\tlineageTag: {uid(name)}\n'
        if name=='dim_date' and calendar_ok:
            body+='\tdataCategory: Time\n'
        for col,typ in fields+EXTRA.get(name,[]):
            body+=f'\n\tcolumn {col}\n\t\tdataType: {typ}\n\t\tlineageTag: {uid(name+"/"+col)}\n\t\tsourceColumn: {col}\n\t\tsummarizeBy: none\n'
            if col.endswith('_key') or col=='group_key' or col.endswith('_order') or (name.startswith('fact_') and typ in ('int64','double')):
                body+='\t\tisHidden\n'
            if typ=='dateTime':
                body+='\t\tformatString: '+('MM/yyyy' if col=='month_start' else 'dd/MM/yyyy')+'\n'
            if name=='dim_date' and col=='date_key' and calendar_ok:
                body+='\t\tisKey\n'
            if name=='dim_date' and col in ['year_month','year_quarter']:
                body+=f'\t\tsortByColumn: {col}_order\n'
            if name=='dim_value_band' and col=='band_label':
                body+='\t\tsortByColumn: band_order\n'
            if typ=='dateTime':
                body+='\t\tannotation UnderlyingDateTimeDataType = Date\n'
        if name=='dim_date':
            body+='\n\thierarchy \'Lịch\'\n\t\tlineageTag: '+uid('calendar')+'\n'
            for ix,(lab,col) in enumerate([('Năm','year'),('Quý','year_quarter'),('Tháng','year_month'),('Ngày','date_key')]):
                body+=f"\t\tlevel '{lab}'\n\t\t\tordinal: {ix}\n\t\t\tcolumn: {col}\n"
        source='let\n    Source = ReadParquetTable("'+name+'", {'+', '.join('"'+c.name+'"' for c in schema)+'}),\n    Typed = Table.TransformColumnTypes(Source, {'+', '.join(mtypes)+'}, "en-US")'
        end='Typed'
        if name=='dim_date':
            source+=',\n    MonthOrder = Table.AddColumn(Typed, "year_month_order", each [year]*100+[month], Int64.Type),\n    QuarterOrder = Table.AddColumn(MonthOrder, "year_quarter_order", each [year]*10+[quarter], Int64.Type),\n    MonthStart = Table.AddColumn(QuarterOrder, "month_start", each Date.StartOfMonth([date_key]), type date)'
            end='MonthStart'
        if name=='dim_geography':
            source+=',\n    Display = Table.AddColumn(Typed,"province_display",each Text.Combine({[province_name],[geography_version],[province_code]}," · "), type text)'
            end='Display'
        if name=='dim_party':
            source+=',\n    Display = Table.AddColumn(Typed,"party_display",each (if [party_name] = null or Text.Trim([party_name]) = "" then "Chưa có tên" else [party_name]) & " [" & [party_key] & "]",type text)'
            end='Display'
        if name=='dim_progress_status':
            source+=',\n    Display = Table.AddColumn(Typed,"status_label",each "TB: " & (if [notice_observation_state]="observed" then "Đã thấy" else "Chưa thấy") & " | KQ: " & (if [award_observation_state]="observed" then "Đã thấy" else "Chưa thấy"), type text)'
            end='Display'
        if name=='dim_duration_metric':
            mapping='['+', '.join(k+'="'+v+'"' for k,v in LABELS.items())+']'
            source+=',\n    Display = Table.AddColumn(Typed,"metric_label",each Record.FieldOrDefault('+mapping+', [metric_key], [metric_name]),type text)'
            end='Display'
        if name=='fact_supplier_daily':
            source+=',\n    Display = Table.AddColumn(Typed,"allocation_label",each Record.FieldOrDefault([single_recipient="Một người nhận",unallocated_consortium="Liên danh chưa phân bổ",independent_winners="Nhiều người nhận độc lập",no_winner="Chưa có người trúng"], [recipient_allocation_state], [recipient_allocation_state]),type text)'
            end='Display'
        source+='\nin\n    '+end
        body+=f'\n\tpartition {name} = m\n\t\tmode: import\n\t\tsource =\n'+''.join('\t\t\t\t'+line+'\n' for line in source.splitlines())
        text(MODEL/f'definition/tables/{name}.tmdl',body)
        definitions[name]={'columns':dict(fields+EXTRA.get(name,[]))}
        if name=='dim_date':definitions[name]['marked_date_table']=calendar_ok
    body='table _Measures\n\tlineageTag: '+uid('_Measures')+'\n'
    for n,m in MEASURES.items():
        body+=f"\n\t/// {m['description']}\n\tmeasure '{n}' = {m['dax']}\n\t\tlineageTag: {uid(n)}\n\t\tdisplayFolder: {m['folder']}\n"
        if m['format'] and n!='Thời gian bình quân':
            body+='\t\tformatString: '+m['format']+'\n'
        if n=='Thời gian bình quân':
            body+='\t\tformatStringDefinition = IF(SELECTEDVALUE(dim_duration_metric[duration_unit]) = "hours", "0.0 \\"giờ\\"", "0.0 \\"ngày\\"")\n'.replace('\\"','""')
    body+='\n\tcolumn _placeholder\n\t\tdataType: int64\n\t\tisHidden\n\t\tsourceColumn: _placeholder\n\n\tpartition _Measures = m\n\t\tmode: import\n\t\tsource = #table(type table [_placeholder = Int64.Type], {{0}})\n'
    text(MODEL/'definition/tables/_Measures.tmdl',body)
    rels=[]
    for n,paths in files.items():
        if not n.startswith('fact_'):
            continue
        for col,(d,k) in FKS.items():
            if col in pq.read_schema(paths[0]).names:
                rels.append(f'relationship {uid(n+"/"+col+"/rel")}\n\tfromColumn: {n}.{col}\n\ttoColumn: {d}.{k}\n\tcrossFilteringBehavior: oneDirection\n')
    text(MODEL/'definition/relationships.tmdl','\n'.join(rels))
    save(PROJECT/'validation/model_inventory.json',{'tables':definitions,'measures':MEASURES,'relationships':len(rels)})


def lit(v):
    s = 'true' if v is True else 'false' if v is False else str(v)+'D' if isinstance(v,(int,float)) else "'"+str(v).replace("'","''")+"'"
    return {'expr':{'Literal':{'Value':s}}}


def color(v):
    return {'solid':{'color':lit(v)}}


def obj(**props):
    return [{'properties':props}]


def field(table,prop,kind='Column',alias=False):
    return {kind:{'Expression':{'SourceRef':{'Source' if alias else 'Entity':table}},'Property':prop}}


def c(t,p): return (t,p,'Column')
def m(n): return ('_Measures',n,'Measure')


def projection(spec):
    t,p,k=spec
    aliases={'province_name':'Địa phương','sector_name':'Lĩnh vực','band_label':'Quy mô gói','year_month':'Tháng','notice_family':'Họ thông báo', 'Dòng · savings':'So giá','Dòng · supplier':'Người nhận','Dòng · notice':'Thông báo','Dòng · status':'Trạng thái','Dòng · demand':'Nhu cầu','Dòng · award':'Kết quả','Dòng · bidding_competition':'Cạnh tranh','Dòng · duration':'Thời gian','province_display':'Địa phương · phiên bản','party_display':'Đơn vị','party_kind':'Loại','allocation_label':'Trạng thái phân bổ','recipient_allocation_state':'Phân bổ','method_category':'Phương thức',
        'Dự toán xác minh · tỷ VNĐ':'Dự toán','Giá trúng xác minh · tỷ VNĐ':'Giá trúng','Tiết kiệm xác minh · tỷ VNĐ':'Tiết kiệm','Tỷ lệ tiết kiệm xác minh':'Tỷ lệ',
        'Giá trị phân bổ · tỷ VNĐ':'Tỷ VNĐ','Giá trị trúng thầu · tỷ VNĐ':'Trúng thầu · tỷ VNĐ','Giá trị kế hoạch · tỷ VNĐ':'Kế hoạch · tỷ VNĐ'}
    result={'field':field(t,p,k),'queryRef':t+'.'+p,'nativeQueryRef':p}
    if p in aliases: result['displayName']=aliases[p]
    return result


def table_widths(v,widths):
    objects=v['visual']['objects']
    objects['columnHeaders']=obj(fontSize=lit(10),autoSizeColumnWidth=lit(False),columnAdjustment=lit('fixedWidth'),wordWrap=lit(True))
    objects['values']=obj(fontSize=lit(10),wordWrap=lit(True))
    projs=[p for role in ['Rows','Values'] for p in v['visual']['query']['queryState'].get(role,{}).get('projections',[])]
    assert len(projs)==len(widths)
    objects['columnWidth']=[{'properties':{'value':lit(width)},'selector':{'metadata':p['queryRef']}} for p,width in zip(projs,widths)]


class Page:
    def add(self,key,typ,x,y,w,h,roles=None,title=None,objects=None,alt=None):
        name=uuid.uuid5(uuid.NAMESPACE_URL,self.key+'/'+key).hex[:20]
        v={'$schema':BASE+'item/report/definition/visualContainer/2.1.0/schema.json','name':name,
           'position':{'x':x,'y':y,'width':w,'height':h,'z':len(self.visuals),'tabOrder':len(self.visuals)},
           'visual':{'visualType':typ,'objects':objects or {}}}
        vc={'background':obj(show=lit(True),color=color('#FFFFFF'),transparency=lit(0)),
            'border':obj(show=lit(False)), 'padding':obj(top=lit(8),bottom=lit(8),left=lit(12),right=lit(12)),
            'visualTooltip':obj(show=lit(True)),
            'general':obj(altText=lit(alt or title or key))}
        if title:
            vc['title']=obj(show=lit(True),text=lit(title),fontSize=lit(11),fontColor=color(INK),fontFamily=lit('Segoe UI Semibold'),titleWrap=lit(True))
        else:
            vc['title']=obj(show=lit(False))
        v['visual']['visualContainerObjects']=vc
        if roles:
            v['visual']['query']={'queryState':{role:{'projections':[projection(s) for s in specs]} for role,specs in roles.items()}}
        self.visuals.append(v)
        return v

    def text(self,key,value,x,y,w,h,size=11,ink=INK):
        v=self.add(key,'textbox',x,y,w,h,objects={'general':obj(paragraphs=[{'textRuns':[{'value':value,'textStyle':{'fontFamily':'Segoe UI','fontSize':f'{size}pt','color':ink}}]}])})
        v['visual']['visualContainerObjects'].update({'background':obj(show=lit(False)),'padding':obj(top=lit(0),bottom=lit(0),left=lit(0),right=lit(0))})
        return v

    def slicer(self,key,spec,label,x,y,w,h,sync=None,select=None):
        objects={'data':obj(mode=lit('Dropdown')),'header':obj(show=lit(True),text=lit(label),textSize=lit(10)),
                 'items':obj(textSize=lit(10)), 'selection':obj(singleSelect=lit(select is not None),selectAllCheckboxEnabled=lit(select is None))}
        if select:
            t,p,k=spec
            objects['general']=obj(filter={'filter':{'Version':2,'From':[{'Name':'d','Entity':t,'Type':0}],
                'Where':[{'Condition':{'In':{'Expressions':[field('d',p,alias=True)],'Values':[[{'Literal':{'Value':"'"+select+"'"}}]]}}}]}})
        v=self.add(key,'slicer',x,y,w,h,roles={'Values':[spec]},objects=objects)
        v['visual']['visualContainerObjects']['padding']=obj(top=lit(0),bottom=lit(0),left=lit(8),right=lit(8))
        if sync:
            v['visual']['syncGroup']={'groupName':sync,'fieldChanges':True,'filterChanges':True}
        return v

    def card(self,key,measure_name,x,y=176,w=296,h=80,label=None,size=24):
        objects={
            'labels':obj(fontSize=lit(size),color=color(NAVY),labelDisplayUnits=lit(1)),
            'categoryLabels':obj(show=lit(False)),
        }
        display_name='Hiển thị · '+measure_name if 'Hiển thị · '+measure_name in MEASURES else measure_name
        v=self.add(key,'card',x,y,w,h,roles={'Values':[m(display_name)]},title=label or measure_name,objects=objects,alt=MEASURES[measure_name]['description'])
        return v

    def chart(self,key,typ,category,measures,x,y,w=608,h=196,title=None,tooltips=None):
        roles={'Category':[category],'Y':[m(n) for n in measures]}
        if tooltips:
            roles['Tooltips']=[m(n) for n in tooltips]
        v=self.add(key,typ,x,y,w,h,roles=roles,title=title,objects={
            'legend':obj(show=lit(len(measures)>1),fontSize=lit(9)),
            'categoryAxis':obj(fontSize=lit(10),showAxisTitle=lit(False),preferredCategoryWidth=lit(14),maxMarginFactor=lit(40)),
            'valueAxis':obj(fontSize=lit(10),showAxisTitle=lit(False),labelDisplayUnits=lit(1)),
            'dataPoint':[{'properties':{'fill':color(SERIES_COLORS.get(n, PALETTE[i % len(PALETTE)]))},'selector':{'metadata':'_Measures.'+n}} for i,n in enumerate(measures)]})
        v['visual']['query']['sortDefinition']={'sort':[{'field':field(*category[:2]) if category[0]=='dim_date' else field('_Measures',measures[0],'Measure'),'direction':'Ascending' if category[0]=='dim_date' else 'Descending'}],'isDefaultSort':True}
        if typ=='lineChart':
            v['visual']['objects']['categoryAxis']=obj(fontSize=lit(10),axisType=lit('Scalar'),showAxisTitle=lit(False))
        return v



def categorical(t,p,values):
    return {'name':uid(t+p+str(values)).replace('-',''),'field':field(t,p),'type':'Categorical','howCreated':'User',
        'filter':{'Version':2,'From':[{'Name':'d','Entity':t,'Type':0}],
        'Where':[{'Condition':{'In':{'Expressions':[field('d',p,alias=True)],'Values':[[{'Literal':{'Value':"'"+x+"'"}}] for x in values]}}}]}}


def report():
    save(REPORT/'definition.pbir',{'$schema':BASE+'item/report/definitionProperties/2.0.0/schema.json','version':'4.0','datasetReference':{'byPath':{'path':'../'+MODEL.name}}})
    save(REPORT/'definition/version.json',{'$schema':BASE+'item/report/definition/versionMetadata/1.0.0/schema.json','version':'2.0.0'})
    theme={'name':'Procurement Enterprise','dataColors':PALETTE,
        'background':BG,'foreground':INK,'tableAccent':TEAL,
        'textClasses':{'title':{'fontFace':'Segoe UI Semibold','fontSize':12,'color':INK},'label':{'fontFace':'Segoe UI','fontSize':10,'color':MUTED},'callout':{'fontFace':'Segoe UI Semibold','fontSize':24,'color':NAVY}},
        'visualStyles':{'*':{'*':{'visualHeader':[{'show':False}],'dropShadow':[{'show':False}]}},
            'tableEx':{'*':{'grid':[{'gridVertical':False,'rowPadding':4}],'columnHeaders':[{'fontSize':10,'fontColor':{'solid':{'color':INK}},'backColor':{'solid':{'color':'#DDF3F2'}}}],'values':[{'fontSize':10}]}},
            'pivotTable':{'*':{'columnHeaders':[{'fontSize':10}],'rowHeaders':[{'fontSize':10}],'values':[{'fontSize':10}]}}}}
    save(PROJECT/'theme.json',theme)
    save(REPORT/'StaticResources/RegisteredResources/ProcurementTheme.json',theme)
    save(REPORT/'definition/report.json',{'$schema':BASE+'item/report/definition/report/3.0.0/schema.json',
        'themeCollection':{'customTheme':{'name':'ProcurementTheme','reportVersionAtImport':{'visual':'2.1.0','page':'2.0.0','report':'3.0.0'},'type':'RegisteredResources'}},
        'resourcePackages':[{'name':'RegisteredResources','type':'RegisteredResources','items':[{'name':'ProcurementTheme','path':'ProcurementTheme.json','type':'CustomTheme'}]}],
        'settings':{'useEnhancedTooltips':True,'hideVisualContainerHeader':True,'exportDataMode':'AllowSummarized','defaultFilterActionIsDataFilter':True}})
    from gold_v3_dashboard import report_pages
    report_pages()



if __name__=='__main__':
    import sys
    import subprocess
    sys.modules['build_project'] = sys.modules[__name__]
    opened = subprocess.run(['powershell', '-NoProfile', '-Command',
        "@(Get-Process PBIDesktop -ErrorAction SilentlyContinue | Where-Object {$_.MainWindowTitle -like '*ProcurementDashboard*'}).Count"],
        capture_output=True, text=True, check=True)
    if int(opened.stdout.strip() or '0'):
        raise SystemExit('Close ProcurementDashboard in Desktop before regenerating its managed files.')
    files=discover(WORKSPACE/'PowerBI/export/parquet')
    define_measures()
    model(files)
    report()
    text(PROJECT/'.gitignore','**/.pbi/\n**/__pycache__/\ntools/authoring/node_modules/\n')
    print(f'Created {len(files)} source tables, {len(MEASURES)} measures, Gold V3 report pages: {PROJECT}')
