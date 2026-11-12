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
    measure('Số gói kế hoạch','SUM(fact_demand_daily[package_count])')
    vn_sum('Giá trị kế hoạch VNĐ','fact_demand_daily','planned_amount_sum',MONEY,sample='eligible_amount_count')
    measure('Giá trị kế hoạch · tỷ VNĐ','DIVIDE([Giá trị kế hoạch VNĐ], 1000000000)',MONEY)
    measure('Số thông báo','SUM(fact_notice_daily[notice_count])',folder='Thông báo')
    measure('Thông báo hủy','SUM(fact_notice_daily[cancelled_notice_count])',folder='Thông báo')
    measure('Tỷ lệ thông báo hủy','DIVIDE([Thông báo hủy], [Số thông báo])',PCT,'Thông báo', 'Số thông báo hủy / số thông báo trong cùng họ notice_family, ngày và lĩnh vực.')
    measure('Số lượt trúng thầu','SUM(fact_award_daily[award_scope_count])',folder='Trúng thầu')
    measure('Số lượt trúng thầu hiệu lực','SUM(fact_award_daily[effective_award_scope_count])',folder='Trúng thầu')
    measure('Số mẫu giá trúng hợp lệ','SUM(fact_award_daily[eligible_amount_count])',folder='Trúng thầu')
    vn_sum('Giá trị trúng thầu VNĐ','fact_award_daily','award_amount_sum',MONEY,'Trúng thầu','eligible_amount_count')
    measure('Giá trị trúng thầu · tỷ VNĐ','DIVIDE([Giá trị trúng thầu VNĐ], 1000000000)',MONEY,'Trúng thầu')
    vn_sum('Số mẫu xác minh','fact_savings_daily','verified_sample_count',COUNT,'Tiết kiệm')
    for n,c in [('Dự toán xác minh','verified_baseline_sum'),('Giá trúng xác minh','verified_award_sum'),('Tiết kiệm xác minh','verified_saving_sum')]:
        vn_sum(n+' VNĐ','fact_savings_daily',c,MONEY,'Tiết kiệm','verified_sample_count')
        measure(n+' · tỷ VNĐ',f'DIVIDE([{n} VNĐ],1000000000)',MONEY,'Tiết kiệm')
    measure('Tỷ lệ tiết kiệm xác minh','DIVIDE([Tiết kiệm xác minh VNĐ], [Dự toán xác minh VNĐ])',PCT,'Tiết kiệm','Tổng tiết kiệm / tổng dự toán, cùng tập mẫu verified và VND; không phải trung bình tỷ lệ theo dòng.')
    vn_sum('Số mẫu hợp đồng','fact_savings_daily','contract_sample_count',COUNT,'Hợp đồng')
    for n,c in [('Giá trị hợp đồng','contract_amount_sum'),('Tiết kiệm hợp đồng','contract_saving_sum')]:
        vn_sum(n+' VNĐ','fact_savings_daily',c,MONEY,'Hợp đồng','contract_sample_count')
        measure(n+' · tỷ VNĐ',f'DIVIDE([{n} VNĐ],1000000000)',MONEY,'Hợp đồng')
    for n,c in [('Số lượt mở thầu','opening_count'),('Tổng lượt nhà thầu','total_bidders'),('Gói một nhà thầu','single_bidder_count'),('Gói nhiều nhà thầu','multi_bidder_count'),('Hồ sơ đạt kỹ thuật','tech_passed_count'),('Hồ sơ không đạt kỹ thuật','tech_failed_count')]:
        measure(n,f'SUM(fact_bidding_competition_daily[{c}])',COUNT,'Cạnh tranh')
    measure('Nhà thầu bình quân','DIVIDE([Tổng lượt nhà thầu], [Số lượt mở thầu])','0.00','Cạnh tranh','Lượt nhà thầu / lượt mở thầu; không phải số đơn vị duy nhất.')
    measure('Tỷ lệ một nhà thầu','DIVIDE([Gói một nhà thầu], [Số lượt mở thầu])',PCT,'Cạnh tranh')
    measure('Tỷ lệ đạt kỹ thuật','DIVIDE([Hồ sơ đạt kỹ thuật], [Hồ sơ đạt kỹ thuật]+[Hồ sơ không đạt kỹ thuật])',PCT,'Cạnh tranh','Trong các hồ sơ có kết quả đạt/không đạt; hồ sơ chưa có kết quả không thuộc mẫu số.')
    valid='KEEPFILTERS(NOT ISBLANK(fact_duration_daily[report_date]))'
    for n,c in [('Số mẫu thời gian hợp lệ','sample_count'),('Số mẫu bị loại có ngày','excluded_sample_count')]:
        measure(n,f'IF(HASONEVALUE(dim_duration_metric[metric_key]), CALCULATE(SUM(fact_duration_daily[{c}]), {valid}))',COUNT,'Thời gian','Chỉ một metric và report_date không trống. Dòng thiếu ngày được giữ trong nguồn nhưng tách khỏi KPI này.')
    measure('Thời gian trung bình',f'IF(HASONEVALUE(dim_duration_metric[metric_key]), DIVIDE(CALCULATE(SUM(fact_duration_daily[duration_sum]), {valid}), [Số mẫu thời gian hợp lệ]))','0.0','Thời gian','Tổng duration_sum / tổng sample_count trên các dòng có ngày, đúng một metric.')
    measure('Đơn vị thời gian','SWITCH(SELECTEDVALUE(dim_duration_metric[duration_unit]), "hours", "giờ", "days", "ngày", BLANK())',None,'Thời gian')
    measure('Metric và đơn vị','IF(HASONEVALUE(dim_duration_metric[metric_key]), SELECTEDVALUE(dim_duration_metric[metric_label]) & " · " & [Đơn vị thời gian], "Chọn đúng một chỉ số")',None,'Thời gian')
    measure('Tiêu đề thời gian','"Thời gian trung bình (" & COALESCE([Đơn vị thời gian], "chọn 1 metric") & ")"',None,'Thời gian')
    measure('Dòng duration thiếu ngày','CALCULATE(COUNTROWS(fact_duration_daily), REMOVEFILTERS(dim_date), ISBLANK(fact_duration_daily[report_date]))',COUNT,'Chất lượng','Số dòng thô thiếu ngày; có grain lặp, không diễn giải thành số mẫu duy nhất. Bỏ qua lọc ngày vì không có ngày để gán.')
    measure('Số gói theo trạng thái','SUM(fact_status_daily[package_count])',COUNT,'Trạng thái','Trạng thái quan sát thông báo/kết quả, không phải tiến độ thực hiện.')
    vn_sum('Giá trị phân bổ đơn VNĐ','fact_supplier_daily','award_amount_sum',MONEY,'Đơn vị','eligible_amount_count')
    MEASURES['Giá trị phân bổ đơn VNĐ']['dax']='CALCULATE('+MEASURES['Giá trị phân bổ đơn VNĐ']['dax']+', KEEPFILTERS(fact_supplier_daily[recipient_allocation_state] = "single_recipient"))'
    MEASURES['Giá trị phân bổ đơn VNĐ']['description']='Chỉ VND, single_recipient, giá trị eligible. Không chia giá trị liên danh hoặc nhiều người nhận cho từng đơn vị.'
    measure('Giá trị phân bổ · tỷ VNĐ','DIVIDE([Giá trị phân bổ đơn VNĐ], 1000000000)',MONEY,'Đơn vị')
    measure('Ghi chú thời gian','"Dòng thiếu ngày của metric (không nằm trong KPI): " & FORMAT(COALESCE([Dòng duration thiếu ngày],0), "#,0", "vi-VN") & ". Chỉ số theo ngày không bao gồm các dòng này."',None,'Chất lượng')
    measure('Loại đơn vị','VAR v=SELECTEDVALUE(dim_party[party_kind]) RETURN SWITCH(v,"ORGANIZATION","Tổ chức","PERSON","Cá nhân","CONSORTIUM","Liên danh",v)',None,'Đơn vị','Nhãn trình bày từ party_kind, không thay đổi dữ liệu gốc.')
    measure('Phân bổ đơn vị','CALCULATE(SWITCH(SELECTEDVALUE(fact_supplier_daily[recipient_allocation_state]),"single_recipient","Nhận đơn",BLANK()), KEEPFILTERS(fact_supplier_daily[recipient_allocation_state]="single_recipient"))',None,'Đơn vị','Nhãn trình bày; bảng chỉ xét trạng thái single_recipient.')
    measure('Phạm vi cạnh tranh','VAR a = MIN(fact_bidding_competition_daily[report_date]) VAR b = MAX(fact_bidding_competition_daily[report_date]) RETURN IF(ISBLANK(a), "Không có dữ liệu cạnh tranh trong bộ lọc hiện tại", "Dữ liệu quan sát: " & FORMAT(a,"dd/MM/yyyy") & " – " & FORMAT(b,"dd/MM/yyyy"))',None,'Cạnh tranh')
    measure('Tình trạng mẫu xác minh','IF(COALESCE([Số mẫu xác minh],0)=0, "Chỉ VND · chưa có mẫu verified hợp lệ trong phạm vi chọn; các chỉ số tiết kiệm để trống.", "Chỉ VND · tiết kiệm tính trên tập mẫu verified; hợp đồng có tập mẫu riêng.")',None,'Chất lượng')
    card_names=['Giá trị kế hoạch · tỷ VNĐ','Giá trị trúng thầu · tỷ VNĐ','Tiết kiệm xác minh · tỷ VNĐ','Tỷ lệ tiết kiệm xác minh','Dự toán xác minh · tỷ VNĐ','Giá trúng xác minh · tỷ VNĐ','Số mẫu xác minh','Số mẫu hợp đồng','Giá trị hợp đồng · tỷ VNĐ','Tiết kiệm hợp đồng · tỷ VNĐ','Số lượt mở thầu','Nhà thầu bình quân','Tỷ lệ một nhà thầu','Tỷ lệ đạt kỹ thuật','Thời gian trung bình','Số mẫu thời gian hợp lệ','Số mẫu bị loại có ngày']
    for n in card_names:
        fmt=MEASURES[n]['format'] or COUNT
        suffix=' & " " & [Đơn vị thời gian]' if n=='Thời gian trung bình' else ''
        measure('Hiển thị · '+n,f'IF(ISBLANK([{n}]), "—", FORMAT([{n}], "{fmt}", "vi-VN"){suffix})',None,'Hiển thị',f'Chỉ dùng trên card; measure số gốc [{n}] dùng cho tính toán và biểu đồ. Dấu phân cách vi-VN; chưa có dữ liệu hiển thị dấu gạch ngang.')


EXTRA = {
 'dim_date': [('year_month_order','int64'),('year_quarter_order','int64'),('month_start','dateTime')],
 'dim_geography': [('province_display','string')],
 'dim_party': [('party_display','string')],
 'dim_duration_metric': [('metric_label','string')],
 'dim_progress_status': [('status_label','string')],
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
        source+='\nin\n    '+end
        body+=f'\n\tpartition {name} = m\n\t\tmode: import\n\t\tsource =\n'+''.join('\t\t\t\t'+line+'\n' for line in source.splitlines())
        text(MODEL/f'definition/tables/{name}.tmdl',body)
        definitions[name]={'columns':dict(fields+EXTRA.get(name,[]))}
        if name=='dim_date':definitions[name]['marked_date_table']=calendar_ok
    body='table _Measures\n\tlineageTag: '+uid('_Measures')+'\n'
    for n,m in MEASURES.items():
        body+=f"\n\t/// {m['description']}\n\tmeasure '{n}' = {m['dax']}\n\t\tlineageTag: {uid(n)}\n\t\tdisplayFolder: {m['folder']}\n"
        if m['format'] and n!='Thời gian trung bình':
            body+='\t\tformatString: '+m['format']+'\n'
        if n=='Thời gian trung bình':
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
    aliases={'province_display':'Địa phương · phiên bản','party_display':'Đơn vị','party_kind':'Loại','recipient_allocation_state':'Phân bổ','method_category':'Phương thức',
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
    def __init__(self,key,title,subtitle):
        self.key=key; self.title=title; self.visuals=[]; self.metrics=[]
        self.doc={'$schema':BASE+'item/report/definition/page/2.0.0/schema.json','name':key,'displayName':title,
            'displayOption':'FitToPage','height':720,'width':1280,
            'objects':{'background':obj(color=color(BG),transparency=lit(0))}}
        self.text('brand', 'Phân tích mua sắm công Việt Nam',24,18,620,22,11,MUTED)
        self.text('heading',title,24,42,620,42,23,NAVY)
        self.add('nav','pageNavigator',688,32,568,48,objects={'text':[{'properties':{'fontSize':lit(9)},'selector':{'id':'default'}}]})
        self.slicer('date',c('dim_date','year_month'),'Tháng · chọn một hoặc nhiều',24,96,296,64,sync='DateSync')
        self.slicer('sector',c('dim_sector','sector_name'),'Lĩnh vực',336,96,296,64,sync='SectorSync')
        self.text('scope',subtitle,648,100,608,56,10,MUTED)

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

    def bound_note(self,key,name,x,y,w,h):
        v=self.add(key,'card',x,y,w,h,roles={'Values':[m(name)]},objects={'labels':obj(fontSize=lit(10),color=color(MUTED),labelDisplayUnits=lit(1)),'categoryLabels':obj(show=lit(False))})
        v['visual']['visualContainerObjects'].update({'background':obj(show=lit(False)),'padding':obj(top=lit(0),bottom=lit(0),left=lit(0),right=lit(0))})
        return v

    def chart(self,key,typ,category,measures,x,y,w=608,h=196,title=None,tooltips=None):
        roles={'Category':[category],'Y':[m(n) for n in measures]}
        if tooltips:
            roles['Tooltips']=[m(n) for n in tooltips]
        v=self.add(key,typ,x,y,w,h,roles=roles,title=title,objects={
            'legend':obj(show=lit(len(measures)>1),fontSize=lit(9)),
            'categoryAxis':obj(fontSize=lit(10),showAxisTitle=lit(False),preferredCategoryWidth=lit(14),maxMarginFactor=lit(40)),
            'valueAxis':obj(fontSize=lit(10),showAxisTitle=lit(False),labelDisplayUnits=lit(1)),
            'dataPoint':[{'properties':{'fill':color([NAVY,TEAL,'#F59E0B','#64748B'][i%4])},'selector':{'metadata':'_Measures.'+n}} for i,n in enumerate(measures)]})
        v['visual']['query']['sortDefinition']={'sort':[{'field':field(*category[:2]) if typ=='lineChart' else field('_Measures',measures[0],'Measure'),'direction':'Ascending' if typ=='lineChart' else 'Descending'}],'isDefaultSort':True}
        if typ=='lineChart':
            v['visual']['objects']['categoryAxis']=obj(fontSize=lit(10),axisType=lit('Scalar'),showAxisTitle=lit(False))
        return v

    def flush(self):
        interactions=[]
        for a in self.visuals:
            for b in self.visuals:
                if a==b:
                    continue
                typ=a['visual']['visualType']
                allowed=typ=='slicer'
                if typ=='slicer' and a['visual'].get('query',{}).get('queryState',{}).get('Values',{}).get('projections',[{}])[0].get('queryRef','').startswith('dim_duration_metric.'):
                    bstr=json.dumps(b,ensure_ascii=False)
                    allowed=any(n in bstr for n in ['Thời gian trung bình','Số mẫu thời gian hợp lệ','Số mẫu bị loại có ngày','Metric và đơn vị','Đơn vị thời gian','Ghi chú thời gian'])
                interactions.append({'source':a['name'],'target':b['name'],'type':'DataFilter' if allowed else 'NoFilter'})
        self.doc['visualInteractions']=interactions
        save(REPORT/f'definition/pages/{self.key}/page.json',self.doc)
        for v in self.visuals:
            save(REPORT/f'definition/pages/{self.key}/visuals/{v["name"]}/visual.json',v)


def categorical(t,p,values):
    return {'name':uid(t+p+str(values)).replace('-',''),'field':field(t,p),'type':'Categorical','howCreated':'User',
        'filter':{'Version':2,'From':[{'Name':'d','Entity':t,'Type':0}],
        'Where':[{'Condition':{'In':{'Expressions':[field('d',p,alias=True)],'Values':[[{'Literal':{'Value':"'"+x+"'"}}] for x in values]}}}]}}


def top10(v,dimension,col,fact,amount,allocation=False):
    # Filter's ranking expression is an aggregation, scoped to VND and (for suppliers) single-recipient.
    wh=[{'Condition':{'In':{'Expressions':[field('c','currency_code',alias=True)],'Values':[[{'Literal':{'Value':"'VND'"}}]]}}}]
    if allocation:
        wh.append({'Condition':{'In':{'Expressions':[field('f','recipient_allocation_state',alias=True)],'Values':[[{'Literal':{'Value':"'single_recipient'"}}]]}}})
    query={'Version':2,'From':[{'Name':'d','Entity':dimension,'Type':0},{'Name':'f','Entity':fact,'Type':0},{'Name':'c','Entity':'dim_currency','Type':0}],
        'Select':[dict(field('d',col,alias=True),Name='field')], 'Where':wh,
        'OrderBy':[{'Direction':2,'Expression':{'Aggregation':{'Expression':field('f',amount,alias=True),'Function':0}}},{'Direction':1,'Expression':field('d',col,alias=True)}], 'Top':10}
    filt={'name':v['name']+'top','field':field(dimension,col),'type':'TopN','howCreated':'User',
        'filter':{'Version':2,'From':[{'Name':'subquery','Expression':{'Subquery':{'Query':query}},'Type':2},{'Name':'d','Entity':dimension,'Type':0}],
        'Where':[{'Condition':{'In':{'Expressions':[field('d',col,alias=True)],'Table':{'SourceRef':{'Source':'subquery'}}}}}]}}
    filters=[filt,categorical('dim_currency','currency_code',['VND'])]
    if allocation:
        filters.append(categorical(fact,'recipient_allocation_state',['single_recipient']))
    v['filterConfig']={'filters':filters}


def report():
    save(REPORT/'definition.pbir',{'$schema':BASE+'item/report/definitionProperties/2.0.0/schema.json','version':'4.0','datasetReference':{'byPath':{'path':'../'+MODEL.name}}})
    save(REPORT/'definition/version.json',{'$schema':BASE+'item/report/definition/versionMetadata/1.0.0/schema.json','version':'2.0.0'})
    theme={'name':'Procurement Enterprise','dataColors':[NAVY,TEAL,'#F59E0B','#64748B','#DC4C4C','#DDF3F2'],
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
    p1=Page('overview','Tổng quan','Chỉ VND · mỗi chỉ số theo ngày sự kiện riêng; không diễn giải tỷ lệ hoàn thành giữa các tổng.')
    for i,n in enumerate(['Giá trị kế hoạch · tỷ VNĐ','Giá trị trúng thầu · tỷ VNĐ','Tiết kiệm xác minh · tỷ VNĐ','Tỷ lệ tiết kiệm xác minh']): p1.card('k'+str(i),n,24+i*312)
    p1.chart('trend','lineChart',c('dim_date','month_start'),['Giá trị kế hoạch · tỷ VNĐ','Giá trị trúng thầu · tỷ VNĐ'],24,272,title='Xu hướng kế hoạch và trúng thầu · tỷ VNĐ')
    p1.chart('sector_breakdown','clusteredColumnChart',c('dim_sector','sector_name'),['Giá trị trúng thầu · tỷ VNĐ'],648,272,title='Giá trị trúng thầu theo lĩnh vực · tỷ VNĐ')
    geo=p1.add('geo','tableEx',24,484,608,196,roles={'Values':[c('dim_geography','province_display'),m('Giá trị trúng thầu · tỷ VNĐ')]},title='Top 10 địa phương · giữ phiên bản địa lý · tỷ VNĐ',objects={'columnHeaders':obj(fontSize=lit(10)),'values':obj(fontSize=lit(10)),'grid':obj(rowPadding=lit(2))})
    geo['visual']['query']['sortDefinition']={'sort':[{'field':field('_Measures','Giá trị trúng thầu · tỷ VNĐ','Measure'),'direction':'Descending'}],'isDefaultSort':True}
    top10(geo,'dim_geography','province_display','fact_award_daily','award_amount_sum')
    table_widths(geo,[360,180])
    p1.chart('status','clusteredBarChart',c('dim_progress_status','status_label'),['Số gói theo trạng thái'],648,484,title='Trạng thái quan sát thông báo (TB) / kết quả (KQ)')
    p1.bound_note('foot','Tình trạng mẫu xác minh',24,692,1232,20)
    p2=Page('savings','Tiết kiệm và hợp đồng','Chỉ VND · tỷ lệ trên tập mẫu verified. Hợp đồng có tập mẫu riêng, không nối thành waterfall.')
    p2.visuals.pop()
    p2.bound_note('scope','Tình trạng mẫu xác minh',648,100,608,56)
    for i,n in enumerate(['Dự toán xác minh · tỷ VNĐ','Giá trúng xác minh · tỷ VNĐ','Tiết kiệm xác minh · tỷ VNĐ','Số mẫu xác minh']): p2.card('k'+str(i),n,24+i*312)
    p2.chart('compare','clusteredColumnChart',c('dim_sector','sector_name'),['Dự toán xác minh · tỷ VNĐ','Giá trúng xác minh · tỷ VNĐ'],24,272,title='Dự toán và giá trúng · cùng tập mẫu · tỷ VNĐ',tooltips=['Số mẫu xác minh','Tỷ lệ tiết kiệm xác minh'])
    p2.add('scatter','scatterChart',648,272,608,196,roles={'Category':[c('dim_geography','province_display')],'X':[m('Dự toán xác minh · tỷ VNĐ')],'Y':[m('Tỷ lệ tiết kiệm xác minh')],'Tooltips':[m('Số mẫu xác minh'),m('Giá trúng xác minh · tỷ VNĐ'),m('Tiết kiệm xác minh · tỷ VNĐ')]},title='Địa phương: dự toán (tỷ VNĐ) và tỷ lệ tiết kiệm')
    matrix=p2.add('matrix','pivotTable',24,484,608,196,roles={'Rows':[c('dim_procurement_method','method_category')],'Values':[m(n) for n in ['Dự toán xác minh · tỷ VNĐ','Giá trúng xác minh · tỷ VNĐ','Tiết kiệm xác minh · tỷ VNĐ','Tỷ lệ tiết kiệm xác minh']]},title='Theo phương thức · giá trị tỷ VNĐ / tỷ lệ %')
    table_widths(matrix,[140,110,110,110,90])
    p2.text('contractlabel','Hợp đồng · tập mẫu riêng, chỉ VND',648,484,608,24,12,NAVY)
    p2.card('contracts','Số mẫu hợp đồng',648,516,192,116,size=20)
    p2.card('contractvalue','Giá trị hợp đồng · tỷ VNĐ',856,516,192,116,label='Giá trị · tỷ VNĐ',size=20)
    p2.card('contractsaving','Tiết kiệm hợp đồng · tỷ VNĐ',1064,516,192,116,label='Tiết kiệm · tỷ VNĐ',size=20)
    p2.text('contractnote','Không tính tỷ lệ hợp đồng khi chưa có dự toán cùng tập mẫu.',648,640,608,40,10,MUTED)
    p2.text('foot','Giá trị chưa có mẫu hợp lệ để trống. Tỷ lệ tiết kiệm = tổng tiết kiệm / tổng dự toán xác minh, không lấy trung bình tỷ lệ.',24,692,1232,20,9,MUTED)
    p3=Page('competition','Cạnh tranh đấu thầu','Phạm vi nguồn: 07–24/01/2025 · lượt nhà thầu không phải đơn vị duy nhất. Không gắn tiền tệ cho fact này.')
    p3.visuals.pop()
    p3.bound_note('scope','Phạm vi cạnh tranh',648,100,608,56)
    for i,n in enumerate(['Số lượt mở thầu','Nhà thầu bình quân','Tỷ lệ một nhà thầu','Tỷ lệ đạt kỹ thuật']): p3.card('k'+str(i),n,24+i*312)
    p3.chart('single','clusteredColumnChart',c('dim_sector','sector_name'),['Gói một nhà thầu','Gói nhiều nhà thầu'],24,272,title='Số gói một và nhiều nhà thầu theo lĩnh vực')
    p3.chart('average','clusteredColumnChart',c('dim_sector','sector_name'),['Nhà thầu bình quân'],648,272,title='Số lượt nhà thầu bình quân / lượt mở thầu')
    competition_trend=p3.chart('trend','lineChart',c('dim_date','month_start'),['Tỷ lệ một nhà thầu'],24,484,title='Tỷ lệ một nhà thầu theo tháng',tooltips=['Số lượt mở thầu','Gói một nhà thầu'])
    competition_trend['visual']['objects']['valueAxis']=obj(fontSize=lit(10),showAxisTitle=lit(False),start=lit(0),end=lit(1))
    p3.chart('technical','clusteredColumnChart',c('dim_sector','sector_name'),['Hồ sơ đạt kỹ thuật','Hồ sơ không đạt kỹ thuật'],648,484,title='Hồ sơ có kết quả kỹ thuật',tooltips=['Tỷ lệ đạt kỹ thuật'])
    p3.text('foot','Một nhà thầu là chỉ báo mức độ cạnh tranh, không kết luận sai phạm. Tỷ lệ đạt chỉ xét hồ sơ có kết quả đạt/không đạt.',24,692,1232,20,9,MUTED)
    p4=Page('duration','Thời gian và đơn vị tham gia','Metric chỉ lọc thời gian · bảng đơn vị chỉ VND, single_recipient. Giữ riêng hai phạm vi phân tích.')
    # Reserve the right half of the filter band for the mandatory single-select metric.
    scope=p4.visuals.pop()
    p4.slicer('metric',c('dim_duration_metric','metric_label'),'Chỉ số thời gian · chọn một',648,96,608,64,select=LABELS['plan_to_notice_days'])
    for i,n in enumerate(['Thời gian trung bình','Số mẫu thời gian hợp lệ','Số mẫu bị loại có ngày']): p4.card('k'+str(i),n,24+i*312)
    p4.card('unit','Đơn vị thời gian',960,label='Đơn vị của metric',size=24)
    bar=p4.chart('durationsector','clusteredColumnChart',c('dim_sector','sector_name'),['Thời gian trung bình'],24,272,title='Thời gian trung bình theo lĩnh vực',tooltips=['Số mẫu thời gian hợp lệ','Số mẫu bị loại có ngày'])
    line=p4.chart('durationtrend','lineChart',c('dim_date','month_start'),['Thời gian trung bình'],24,484,title='Xu hướng thời gian trung bình',tooltips=['Số mẫu thời gian hợp lệ'])
    for v in (bar,line):
        v['visual']['visualContainerObjects']['title'][0]['properties']['text']={'expr':field('_Measures','Tiêu đề thời gian','Measure')}
    table=p4.add('supplier','tableEx',648,272,608,408,roles={'Values':[c('dim_party','party_display'),m('Loại đơn vị'),m('Phân bổ đơn vị'),m('Giá trị phân bổ · tỷ VNĐ')]},title='Top 10 đơn vị nhận đơn · tỷ VNĐ',alt='Chỉ single_recipient và VND. Không phân bổ liên danh, nhiều người nhận; metric thời gian không lọc bảng này.',objects={'columnHeaders':obj(fontSize=lit(10),autoSizeColumnWidth=lit(True)),'values':obj(fontSize=lit(10)),'grid':obj(rowPadding=lit(4))})
    top10(table,'dim_party','party_display','fact_supplier_daily','award_amount_sum',True)
    table_widths(table,[270,85,85,124])
    table['visual']['query']['sortDefinition']={'sort':[{'field':field('_Measures','Giá trị phân bổ · tỷ VNĐ','Measure'),'direction':'Descending'}],'isDefaultSort':True}
    p4.bound_note('foot','Ghi chú thời gian',24,692,1232,20)
    pages=[p1,p2,p3,p4]
    save(REPORT/'definition/pages/pages.json',{'$schema':BASE+'item/report/definition/pagesMetadata/1.0.0/schema.json','pageOrder':[p.key for p in pages],'activePageName':'overview'})
    for p in pages: p.flush()
    save(PROJECT/'validation/report_inventory.json',{'pages':[{'name':p.key,'displayName':p.title,'visuals':len(p.visuals)} for p in pages]})


if __name__=='__main__':
    files=discover(WORKSPACE/'PowerBI/export/parquet')
    define_measures()
    model(files)
    report()
    text(PROJECT/'.gitignore','**/.pbi/\n**/__pycache__/\ntools/authoring/node_modules/\n')
    print(f'Created {len(files)} source tables, {len(MEASURES)} measures, four report pages: {PROJECT}')
