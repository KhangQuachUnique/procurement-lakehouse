"""Supported Gold V3 measures and pages; excluded source metrics stay out of the report."""
import json
import shutil

import build_project as b


SUPPORTED_METRICS = ['plan_to_notice_days', 'notice_to_close_hours',
                     'close_to_first_award_days', 'close_to_open_hours', 'open_to_award_days']


def define_measures():
    b.MEASURES.clear()
    add = b.measure
    money = b.MONEY
    pct = b.PCT
    for name, fact, col in [
        ('Số gói kế hoạch', 'fact_demand_daily', 'package_count'),
        ('Mẫu giá kế hoạch', 'fact_demand_daily', 'eligible_amount_count'),
        ('Lượt kết quả', 'fact_award_daily', 'award_scope_count'),
        ('Lượt kết quả hiệu lực', 'fact_award_daily', 'effective_award_scope_count'),
        ('Mẫu giá trúng', 'fact_award_daily', 'eligible_amount_count'),
        ('Mẫu mô tả', 'fact_savings_daily', 'descriptive_sample_count'),
        ('Phạm vi so sánh', 'fact_savings_daily', 'scope_count'),
        ('Mẫu xác minh', 'fact_savings_daily', 'verified_sample_count'),
        ('Mẫu hợp đồng', 'fact_savings_daily', 'contract_sample_count'),
        ('Lượt mở thầu', 'fact_bidding_competition_daily', 'opening_count'),
        ('Lượt nhà thầu', 'fact_bidding_competition_daily', 'total_bidders'),
        ('Lượt một nhà thầu', 'fact_bidding_competition_daily', 'single_bidder_count'),
        ('Lượt nhiều nhà thầu', 'fact_bidding_competition_daily', 'multi_bidder_count'),
        ('Thông báo', 'fact_notice_daily', 'notice_count'),
        ('Thông báo hủy quan sát', 'fact_notice_daily', 'cancelled_notice_count'),
    ]:
        add(name, f'SUM({fact}[{col}])', folder='Số lượng', desc=f'Tổng {fact}[{col}] trong phạm vi chọn; không phải số dòng tổng hợp.')
    for name, fact, col, sample in [
        ('Kế hoạch', 'fact_demand_daily', 'planned_amount_sum', 'eligible_amount_count'),
        ('Trúng thầu', 'fact_award_daily', 'award_amount_sum', 'eligible_amount_count'),
        ('Dự toán mô tả', 'fact_savings_daily', 'descriptive_baseline_sum', 'descriptive_sample_count'),
        ('Giá trúng mô tả', 'fact_savings_daily', 'descriptive_award_sum', 'descriptive_sample_count'),
        ('Chênh lệch mô tả', 'fact_savings_daily', 'descriptive_difference_sum', 'descriptive_sample_count'),
    ]:
        b.vn_sum(name + ' VND', fact, col, money, 'Giá trị', sample)
        add(name + ' · tỷ VND', f'DIVIDE([{name} VND], 1000000000)', money, 'Giá trị')
    for name, dax, fmt in [
        ('Kế hoạch bình quân · tỷ VND', 'DIVIDE([Kế hoạch · tỷ VND], [Mẫu giá kế hoạch])', money),
        ('Độ phủ giá trúng', 'DIVIDE([Mẫu giá trúng], [Lượt kết quả hiệu lực])', pct),
        ('Tỷ lệ chênh lệch mô tả', 'DIVIDE([Chênh lệch mô tả VND], [Dự toán mô tả VND])', pct),
        ('Độ phủ mẫu mô tả', 'DIVIDE([Mẫu mô tả], [Phạm vi so sánh])', pct),
        ('Nhà thầu bình quân', 'DIVIDE([Lượt nhà thầu], [Lượt mở thầu])', '0.00'),
        ('Tỷ lệ một nhà thầu', 'DIVIDE([Lượt một nhà thầu], [Lượt mở thầu])', pct),
        ('Tỷ lệ nhiều nhà thầu', 'DIVIDE([Lượt nhiều nhà thầu], [Lượt mở thầu])', pct),
        ('Tỷ lệ hủy quan sát', 'DIVIDE([Thông báo hủy quan sát], [Thông báo])', pct),
    ]:
        add(name, dax, fmt, 'Tỷ lệ và bình quân')
    add('Mẫu giá trúng VND', 'CALCULATE([Mẫu giá trúng], KEEPFILTERS(dim_currency[currency_code]="VND"))', folder='Giá trị')
    for name, base in [('Tỷ trọng gói VND', 'Số gói kế hoạch'), ('Tỷ trọng giá kế hoạch VND', 'Kế hoạch · tỷ VND')]:
        add(name, f'CALCULATE(DIVIDE([{base}], CALCULATE([{base}], ALLSELECTED(dim_value_band[band_label], dim_value_band[band_order]))), KEEPFILTERS(dim_currency[currency_code]="VND"))', pct, 'Cơ cấu')
    add('Chênh so với tập chọn · đpt', 'VAR r=[Tỷ lệ một nhà thầu] VAR refRate=CALCULATE([Tỷ lệ một nhà thầu], ALLSELECTED(dim_sector[sector_name])) RETURN IF(NOT ISBLANK(r) && NOT ISBLANK(refRate), (r-refRate)*100)', '0.0', 'Cạnh tranh')
    add('Gói theo trạng thái quan sát',
        'VAR lastDay=CALCULATE(MAX(fact_status_daily[report_date]), REMOVEFILTERS(dim_progress_status)) RETURN IF(NOT ISBLANK(lastDay), CALCULATE(SUM(fact_status_daily[package_count]), KEEPFILTERS(fact_status_daily[report_date]=lastDay)))',
        folder='Số lượng', desc='Tổng theo trạng thái tại ngày cuối có bản ghi trong phạm vi tháng và lĩnh vực đang chọn.')
    metric_guard = 'HASONEVALUE(dim_duration_metric[metric_key])'
    for name, col in [('Mẫu thời gian', 'sample_count'), ('Mẫu thời gian bị loại', 'excluded_sample_count'), ('Khoảng thời gian', 'interval_count')]:
        add(name, f'IF({metric_guard}, SUM(fact_duration_daily[{col}]))', folder='Thời gian')
    add('Thời gian bình quân', f'IF({metric_guard}, DIVIDE(SUM(fact_duration_daily[duration_sum]), [Mẫu thời gian]))', '0.0', 'Thời gian')
    add('Tỷ lệ mẫu bị loại', 'DIVIDE([Mẫu thời gian bị loại], [Khoảng thời gian])', pct, 'Thời gian')
    add('Đơn vị thời gian', 'SWITCH(SELECTEDVALUE(dim_duration_metric[duration_unit]), "hours", "giờ", "days", "ngày", BLANK())', None, 'Thời gian')

    supplier_filter = ('KEEPFILTERS(fact_supplier_daily[recipient_allocation_state]="single_recipient"), '
                       'KEEPFILTERS(fact_supplier_daily[eligible_amount_count]>0), '
                       'KEEPFILTERS(dim_party[party_kind]<>"UNKNOWN"), KEEPFILTERS(dim_currency[currency_code]="VND")')
    add('Người nhận · tỷ VND', f'CALCULATE(IF(SUM(fact_supplier_daily[eligible_amount_count])>0, DIVIDE(SUM(fact_supplier_daily[award_amount_sum]),1000000000)), {supplier_filter})', money, 'Người nhận')
    add('Số khóa người nhận', f'CALCULATE(DISTINCTCOUNT(fact_supplier_daily[party_key]), {supplier_filter})', folder='Người nhận')
    add('Mẫu giá người nhận', f'CALCULATE(SUM(fact_supplier_daily[eligible_amount_count]), {supplier_filter})', folder='Người nhận')
    add('Phân bổ · tỷ VND', 'CALCULATE(IF(SUM(fact_supplier_daily[eligible_amount_count])>0, DIVIDE(SUM(fact_supplier_daily[award_amount_sum]),1000000000)), KEEPFILTERS(dim_currency[currency_code]="VND"))', money, 'Người nhận')
    add('Phân bổ · số lượt', 'SUM(fact_supplier_daily[award_scope_count])', folder='Người nhận')
    add('Độ phủ giá trị người nhận', 'DIVIDE([Người nhận · tỷ VND], [Phân bổ · tỷ VND])', pct, 'Người nhận')
    # Ranking removes only party identity, preserving the selected market and eligible population.
    add('Tỷ trọng Top 10 người nhận', f'''CALCULATE(
        VAR People = CALCULATETABLE(VALUES(fact_supplier_daily[party_key]), {supplier_filter})
        VAR Amounts = FILTER(ADDCOLUMNS(People, "_value", [Người nhận · tỷ VND]), NOT ISBLANK([_value]))
        VAR Ranked = TOPN(10, Amounts, [_value], DESC, fact_supplier_daily[party_key], ASC)
        RETURN DIVIDE(SUMX(Ranked, [_value]), [Người nhận · tỷ VND]), ALLSELECTED(dim_party))'''.replace('\n', ' '), pct, 'Người nhận')
    add('Nhóm kỹ thuật cần xác minh', 'COUNTROWS(FILTER(fact_bidding_competition_daily, fact_bidding_competition_daily[tech_passed_count]+fact_bidding_competition_daily[tech_failed_count]>fact_bidding_competition_daily[total_bidders]))', folder='Chất lượng')
    add('Nhóm tiền dự thầu lệch', 'COUNTROWS(FILTER(fact_bidding_competition_daily, ABS(fact_bidding_competition_daily[total_bid_amount]-fact_bidding_competition_daily[total_discount_amount]-fact_bidding_competition_daily[total_final_bid_amount])>0.01))', folder='Chất lượng', desc='Số nhóm lệch trên 0,01 đơn vị tiền nguồn sau chuyển double; không phải chênh lệch đã quy đổi VND.')
    add('Phạm vi cạnh tranh', 'VAR a=MIN(fact_bidding_competition_daily[report_date]) VAR z=MAX(fact_bidding_competition_daily[report_date]) RETURN IF(ISBLANK(a), "Không có dữ liệu trong phạm vi chọn", "Quan sát: " & FORMAT(a,"dd/MM/yyyy") & " – " & FORMAT(z,"dd/MM/yyyy"))', None, 'Chất lượng')
    for fact in ['demand', 'award', 'savings', 'bidding_competition', 'duration', 'supplier', 'notice', 'status']:
        add('Dòng · ' + fact, f'COUNTROWS(fact_{fact}_daily)', folder='Độ phủ', desc='Số nhóm tổng hợp có bản ghi; không chứng minh độ đầy đủ thu thập.')
    labels = {'demand': 'Kế hoạch', 'award': 'Kết quả', 'savings': 'So sánh giá',
              'bidding_competition': 'Mở thầu', 'duration': 'Thời gian', 'supplier': 'Người nhận',
              'notice': 'Thông báo', 'status': 'Trạng thái'}
    observed = ', '.join(f'IF([Dòng · {fact}] > 0, "{label}")' for fact, label in labels.items())
    add('Luồng có quan sát', 'VAR streams=FILTER({' + observed + '}, NOT ISBLANK([Value])) RETURN IF(COUNTROWS(streams)>0, CONCATENATEX(streams,[Value],", ",[Value],ASC))', None, 'Độ phủ')
    add('Đối sánh tập chọn', 'CALCULATE([Tỷ lệ một nhà thầu], ALLSELECTED(dim_sector[sector_name]))', pct, 'Cạnh tranh')
    add('Màu cạnh tranh', 'VAR r=[Tỷ lệ một nhà thầu] RETURN IF(NOT ISBLANK(r), SWITCH(TRUE(), [Lượt mở thầu]<30,"#FFF0D4", r<0.25,"#EFF5FC", r<0.5,"#D5E6FA", r<0.75,"#A8CDF5","#76A8E0"))', None, 'Cạnh tranh')
    add('Lưu ý cỡ mẫu', 'IF([Lượt mở thầu]>0 && [Lượt mở thầu]<30, "Ít hơn 30 lượt mở")', None, 'Cạnh tranh')
    add('Mẫu ngoài so sánh', 'IF(NOT ISBLANK([Phạm vi so sánh]) && NOT ISBLANK([Mẫu mô tả]), [Phạm vi so sánh]-[Mẫu mô tả])', folder='Chất lượng')
    add('Khóa người nhận', 'SELECTEDVALUE(dim_party[party_key])', None, 'Người nhận')
    add('Giá trúng thiếu địa phương', 'CALCULATE([Trúng thầu · tỷ VND], KEEPFILTERS(dim_geography[geography_key] IN {"GEO_UNKNOWN","GEO_MULTI_UNALLOCATED"}))', money, 'Chất lượng')
    add('Giá chưa phân bổ', 'CALCULATE([Phân bổ · tỷ VND], KEEPFILTERS(fact_supplier_daily[recipient_allocation_state]<>"single_recipient"))', money, 'Chất lượng')
    # Use the full selected VND population as denominator before applying the size band.
    for label, base in [('Số gói', 'Số gói kế hoạch'), ('Giá trị', 'Kế hoạch · tỷ VND')]:
        add(label + ' nhóm từ 100 tỷ', f'CALCULATE(DIVIDE(CALCULATE([{base}], KEEPFILTERS(dim_value_band[band_label]="Từ 100 tỷ VND trở lên")), [{base}]), KEEPFILTERS(dim_currency[currency_code]="VND"))', pct, 'Phân tích')
    add('Biên độ cạnh tranh', 'VAR sectors=FILTER(ALLSELECTED(dim_sector[sector_name]), [Lượt mở thầu]>=30) RETURN IF(COUNTROWS(sectors)>1, (MAXX(sectors,[Tỷ lệ một nhà thầu])-MINX(sectors,[Tỷ lệ một nhà thầu]))*100)', '0.0', 'Phân tích')
    for name in list(b.MEASURES):
        if name.startswith('Dòng · '):
            add('Nền ' + name, f'IF(NOT ISBLANK([{name}]), "#DCEAFE", "#F5F6F8")', None, 'Phân tích')
    add('Tên người nhận', 'SELECTEDVALUE(dim_party[party_name])', None, 'Hiển thị')
    add('Hạng người nhận', 'VAR k=SELECTEDVALUE(dim_party[party_key]) VAR v=[Người nhận · tỷ VND] VAR candidates=CALCULATETABLE(VALUES(dim_party[party_key]),ALLSELECTED(dim_party[party_key])) VAR ranked=ADDCOLUMNS(candidates,"_amount",CALCULATE([Người nhận · tỷ VND])) RETURN IF(ISINSCOPE(dim_party[party_key]) && NOT ISBLANK(v),1+COUNTROWS(FILTER(ranked,[_amount]>v || ([_amount]=v && dim_party[party_key]<k))))', None, 'Hiển thị')
    for key, title in [('trend','Bình quân theo tháng'), ('sectors','Bình quân theo lĩnh vực'), ('methods','Bình quân theo phương thức')]:
        add('Tiêu đề thời gian ' + key, f'"{title}" & IF(NOT ISBLANK([Đơn vị thời gian]), " · " & [Đơn vị thời gian])', None, 'Hiển thị')
    add('Tiêu đề cơ cấu lĩnh vực', 'VAR v=[Trúng thầu · tỷ VND] RETURN "Giá trúng theo lĩnh vực" & IF(NOT ISBLANK(v), " · " & FORMAT(v/1000,"#,0.0","vi-VN") & " nghìn tỷ VND")', None, 'Hiển thị')
    # Cards use explicit Vietnamese formatting; numerical originals remain available to charts.
    for name, definition in list(b.MEASURES.items()):
        if definition['format'] is None:
            continue
        suffix = ' & " " & [Đơn vị thời gian]' if name == 'Thời gian bình quân' else ''
        add('Hiển thị · ' + name, f'IF(ISBLANK([{name}]), "—", FORMAT([{name}], "{definition["format"]}", "vi-VN"){suffix})', None, 'Hiển thị')

    for key, (_, guard, expression, _) in NARRATIVES.items():
        add('Nhận định · '+key, f'IF(ISBLANK([{guard}]), "Chưa đủ quan sát để diễn giải trong phạm vi đã chọn.", {expression})', None, 'Hiển thị')
    for name in ['Kế hoạch · tỷ VND','Trúng thầu · tỷ VND','Dự toán mô tả · tỷ VND','Giá trúng mô tả · tỷ VND','Chênh lệch mô tả · tỷ VND','Người nhận · tỷ VND','Giá trúng thiếu địa phương','Giá chưa phân bổ']:
        add('KPI · '+name, f'VAR v=[{name}] RETURN IF(ISBLANK(v),"—",IF(ABS(v)>=1000,FORMAT(v/1000,"#,0.0","vi-VN") & " nghìn tỷ",FORMAT(v,"#,0.0","vi-VN") & " tỷ"))', None, 'Hiển thị')


SIDEBAR = '#14243D'
ICONS = {
    'overview':'M3 20h18 M5 16v-5h3v5z M11 16V7h3v9z M17 16V3h3v13z',
    'competition':'M3 7h12m-3-3 3 3-3 3 M21 17H9m3-3-3 3 3 3',
    'savings':'M5 4h14v16H5z M8 8h8 M8 12h3 M14 12h2 M8 16h3 M14 16h2',
    'duration':'M12 3a9 9 0 1 0 .01 0 M12 7v5l4 2',
    'suppliers':'M4 20v-9l8-5 8 5v9z M8 20v-6h8v6 M9 10h6',
    'quality':'M12 3l8 3v6c0 4-4 7-8 9-4-2-8-5-8-9V6z M8 12l3 3 5-6',
}
RESOURCE_FILES = {}

def svg_resource(name, body, width, height):
    filename=name+'.svg'
    b.text(b.REPORT/'StaticResources/RegisteredResources'/filename,
           f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">{body}</svg>')
    RESOURCE_FILES[name]={'name':name,'path':filename,'type':'Image'}
    return {'expr':{'ResourcePackageItem':{'PackageName':'RegisteredResources','PackageType':1,'ItemName':name}}}

LEFT, RIGHT = 228, 1576
PAGE_TITLES = {
    'overview': ('Tổng quan thị trường', 'Quy mô thị trường đấu thầu công', 'Quy mô, xu hướng và cơ cấu giá trị trong phạm vi đang chọn.'),
    'competition': ('Mức độ cạnh tranh', 'Mức độ cạnh tranh', 'So sánh tỷ lệ một nhà thầu theo thời gian, lĩnh vực và phương thức.'),
    'savings': ('Dự toán và giá trúng', 'Dự toán và giá trúng', 'Giá trị VND trên cùng tập kết quả đủ điều kiện so sánh.'),
    'duration': ('Thời gian quy trình', 'Thời gian quy trình', 'Chọn một khoảng thời gian để so sánh bình quân và chất lượng mẫu.'),
    'suppliers': ('Thị trường nhà cung cấp', 'Thị trường nhà cung cấp', 'Giá trị phân bổ, mức tập trung và các đơn vị nhận thầu.'),
    'quality': ('Chất lượng dữ liệu', 'Độ phủ và chất lượng dữ liệu', 'Theo dõi phạm vi quan sát, mẫu đủ điều kiện và giá trị thiếu phân bổ.'),
}
NARRATIVES = {
 'overview': ('Giá trị tập trung ở đâu?', 'Giá trị nhóm từ 100 tỷ', '"Nhóm từ 100 tỷ VND chiếm " & [Hiển thị · Số gói nhóm từ 100 tỷ] & " số gói và " & [Hiển thị · Giá trị nhóm từ 100 tỷ] & " giá trị kế hoạch."', 'Hai tỷ trọng cùng tính trên phạm vi VND đang chọn.'),
 'competition': ('Khoảng cách giữa lĩnh vực','Biên độ cạnh tranh','"Tỷ lệ một nhà thầu giữa lĩnh vực cao nhất và thấp nhất cách nhau " & [Hiển thị · Biên độ cạnh tranh] & " điểm phần trăm."','So sánh các lĩnh vực có từ 30 lượt mở. Ô màu kem đánh dấu mẫu nhỏ.'),
 'savings': ('Phạm vi so sánh giá','Tỷ lệ chênh lệch mô tả','"Trên " & [Hiển thị · Mẫu mô tả] & " mẫu đủ điều kiện, chênh lệch dự toán trừ giá trúng bằng " & [Hiển thị · Tỷ lệ chênh lệch mô tả] & " dự toán."','Chênh lệch quan sát cần được xác minh trước khi diễn giải là tiết kiệm.'),
 'duration': ('Thời gian quan sát','Thời gian bình quân','"Bình quân " & [Hiển thị · Thời gian bình quân] & " dựa trên " & [Hiển thị · Mẫu thời gian] & " khoảng hợp lệ. Tỷ lệ bị loại là " & [Hiển thị · Tỷ lệ mẫu bị loại] & "."','Bộ chọn chỉ số xác định hai mốc thời gian và đơn vị đo.'),
 'suppliers': ('Mức tập trung giá trị','Tỷ trọng Top 10 người nhận','"Mười người nhận đứng đầu chiếm " & [Hiển thị · Tỷ trọng Top 10 người nhận] & " giá trị trong tập xếp hạng. Giá trị xác định người nhận đạt " & [Hiển thị · Độ phủ giá trị người nhận] & " tổng phân bổ."','Xếp hạng theo định danh trên mẫu VND đủ điều kiện.'),
 'quality': ('Khả năng sử dụng dữ liệu','Phạm vi so sánh','"Mẫu đủ điều kiện so giá chiếm " & [Hiển thị · Độ phủ mẫu mô tả] & " phạm vi so sánh. Trong tập kết quả hiệu lực, độ phủ giá trúng là " & [Hiển thị · Độ phủ giá trúng] & "."','Độ phủ dùng mẫu số riêng cho từng chỉ tiêu.'),
}

class Page(b.Page):
    def __init__(self, key, currency=False):
        self.key = key
        self.title, headline, description = PAGE_TITLES[key]
        self.visuals = []
        self.doc = {'$schema': b.BASE+'item/report/definition/page/2.0.0/schema.json',
                    'name': key, 'displayName': self.title, 'displayOption': 'FitToPage',
                    'height': 900, 'width': 1600,
                    'objects': {'background': b.obj(color=b.color(b.BG), transparency=b.lit(0))}}
        mark='<path d="M7 14l14-8 14 8v16l-14 8-14-8z M7 14l14 8 14-8 M21 22v16" fill="none" stroke="#A8CDF5" stroke-width="2.4" stroke-linejoin="round"/>'
        wordmark='<text x="46" y="20" font-family="Segoe UI, sans-serif" font-size="16" font-weight="600" fill="white">Procurement<tspan x="46" dy="21">Analytics</tspan></text>'
        self.picture('brand',svg_resource('procurement-brand',mark+wordmark,184,50),0,0,204,98,SIDEBAR)
        self.block('brand_sub', 'Đấu thầu công Việt Nam', 0, 98, 204, 63, 11, '#D6E4F7', SIDEBAR, 16)
        for i,page in enumerate(PAGE_TITLES):
            self.icon('icon_'+page,page,0,161+i*54,31,54,'active' if page==key else 'nav')
        nav = self.add('nav','pageNavigator',31,161,173,324,objects={
            'layout':b.obj(orientation=b.lit(1),cellPadding=b.lit(0)),
            'text':[{'selector':{'id':state},'properties':{'fontSize':b.lit(11),'fontColor':b.color(ink),
                    'horizontalAlignment':b.lit('left'),'leftMargin':b.lit(4)}} for state,ink in [('default','#FFFFFF'),('selected',SIDEBAR)]],
            'fill':[{'selector':{'id':state},'properties':{'show':b.lit(True),'fillColor':b.color(fill)}}
                    for state,fill in [('default',SIDEBAR),('selected','#E8F1FF')]],
            'outline':b.obj(show=b.lit(False))})
        self.surface(nav,SIDEBAR,False)
        nav['visual']['visualContainerObjects']['padding']=b.obj(**{side:b.lit(0) for side in ('top','bottom','left','right')})
        self.block('rail', '', 0, 485, 204, 287, 11, '#FFFFFF', SIDEBAR)
        self.block('range','PHẠM VI DỮ LIỆU\n09/2022 – 12/2025',0,772,204,128,11,'#D6E4F7',SIDEBAR,16)
        self.text('section', self.title.upper(),LEFT,14,670,23,11,b.NAVY)
        heading=self.text('heading',headline,LEFT,39,778,57,30,b.INK)
        heading['visual']['objects']['general'][0]['properties']['paragraphs'][0]['textRuns'][0]['textStyle']['fontWeight']='bold'
        self.text('scope',description,LEFT,98,735,38,12,b.MUTED)
        self.slicer('date',b.c('dim_date','year_month'),'Tháng',1008,12,274,52,sync='DateSync')
        self.slicer('sector',b.c('dim_sector','sector_name'),'Lĩnh vực',1296,12,280,52,sync='SectorSync')
        self.doc['filterConfig'] = {'filters': []}
        if currency:
            self.doc['filterConfig']['filters'].append(b.categorical('dim_currency', 'currency_code', ['VND']))

    @staticmethod
    def surface(v, fill, border=True):
        vc=v['visual']['visualContainerObjects']
        vc['background']=b.obj(show=b.lit(True),color=b.color(fill),transparency=b.lit(0))
        vc['border']=b.obj(show=b.lit(border),color=b.color('#E2E8F0'),radius=b.lit(8))

    def text(self,*args):
        v=super().text(*args)
        v['visual']['visualContainerObjects']['border']=b.obj(show=b.lit(False))
        return v

    def block(self,key,value,x,y,w,h,size,ink,fill,padding=8):
        v=self.text(key,value,x,y,w,h,size,ink)
        self.surface(v,fill,False)
        v['visual']['visualContainerObjects']['padding']=b.obj(left=b.lit(padding),top=b.lit(padding))
        return v

    def picture(self,key,resource,x,y,w,h,fill):
        v=self.add(key,'image',x,y,w,h,objects={'general':b.obj(imageUrl=resource),'imageScaling':b.obj(imageScalingType=b.lit('Normal'))})
        self.surface(v,fill,False)
        v['visual']['visualContainerObjects']['padding']=b.obj(**{side:b.lit(0) for side in ('left','right','top','bottom')})
        return v

    def icon(self,key,symbol,x,y,w,h,style):
        fill={'active':'#E8F1FF','nav':SIDEBAR,'badge':'#FFFFFF','focus':'#EAF1FF'}[style]
        ink='#D0E4FF' if style=='nav' else b.NAVY
        circle='<circle cx="12" cy="12" r="17" fill="#DBE9FC"/>' if style in ('badge','focus') else ''
        body=f'<g transform="translate({(w-24)/2} {(h-24)/2})">{circle}<path d="{ICONS[symbol]}" stroke="{ink}" stroke-width="1.65" stroke-linecap="round" stroke-linejoin="round" fill="none"/></g>'
        return self.picture(key,svg_resource(symbol+'-'+style,body,w,h),x,y,w,h,fill)

    def filters(self, geography=False, method=False, band=False):
        if geography:
            self.slicer('geography', b.c('dim_geography', 'province_name'), 'Địa phương theo nguồn', 1008, 74, 274, 52)
        if method:
            self.slicer('method', b.c('dim_procurement_method', 'method_category'), 'Phương thức', 1296, 74, 280, 52)
        elif band:
            self.slicer('band', b.c('dim_value_band', 'band_label'), 'Quy mô gói', 1296, 74, 280, 52)

    def cards(self, names):
        for i, name in enumerate(names):
            width=(RIGHT-LEFT-12*(len(names)-1))//len(names)
            focus={'overview':1,'competition':2,'savings':3,'duration':0,'suppliers':3,'quality':0}
            selected=i==focus[self.key]
            x=LEFT+i*(width+12)
            self.icon('badge'+str(i),('overview','savings','quality','competition')[i],x,152,46,104,'focus' if selected else 'badge')
            compact='KPI · '+name
            label={'Giá trúng thiếu địa phương':'Giá thiếu địa phương','Giá chưa phân bổ':'Ngoài một người nhận'}.get(name,name.split(' · ')[0])
            v=self.card('k'+str(i),compact if compact in b.MEASURES else name,x+46,152,width-46,104,label=label,size=24)
            self.surface(v,'#EAF1FF' if selected else '#FFFFFF',False)
            v['visual']['objects']['labels'][0]['properties'].update(bold=b.lit(True),fontFamily=b.lit('Segoe UI Semibold'))
            subtitle={'overview':'Thị trường đang chọn','competition':'Trên tập lượt mở','savings':'Mẫu so sánh VND','duration':'Theo khoảng đang chọn','suppliers':'Phân bổ giá trị VND','quality':'Giá trị trúng bằng VND'}[self.key]
            v['visual']['visualContainerObjects']['subTitle']=b.obj(show=b.lit(True),text=b.lit(subtitle),fontSize=b.lit(9),fontColor=b.color(b.MUTED))
            v['visual']['visualContainerObjects']['general'][0]['properties']['altText']={'expr':b.field('_Measures','Hiển thị · '+name,'Measure')}

    def chart(self, key, typ, category, measures, x, y, w=684, h=248, title=None, tooltips=None):
        v=super().chart(key, typ, category, measures, x, y, w, h, title, tooltips)
        if typ=='clusteredBarChart' and len(measures)==1:
            v['visual']['objects']['labels']=b.obj(show=b.lit(True),fontSize=b.lit(10),labelPrecision=b.lit(1),color=b.color(b.INK))
        if self.key=='duration' and key in ('trend','sectors','methods'):
            v['visual']['visualContainerObjects']['title'][0]['properties']['text']={'expr':b.field('_Measures','Tiêu đề thời gian '+key,'Measure')}
        return v

    def matrix(self, key, roles, title, x, y, w, h, shades):
        objects = {'values': []}
        for measure, expression in shades.items():
            objects['values'].append({'selector': {'metadata': '_Measures.' + measure,
                'data': [{'dataViewWildcard': {'matchingOption': 1}}]},
                'properties': {'backColor': {'solid': {'color': expression}}}})
        return self.add(key, 'pivotTable', x, y, w, h, roles=roles, title=title, objects=objects)

    def insight(self):
        headline, _, _, explanation=NARRATIVES[self.key]
        x,w=1290,286
        self.block('insight_title',headline,x,272,w,66,15,'#935009','#FFF8ED',12)
        v=self.block('insight_body','',x,338,w,156,13,b.INK,'#FFF8ED',12)
        v['visual']['visualContainerObjects']['title']=b.obj(show=b.lit(True),titleWrap=b.lit(True),
            text={'expr':b.field('_Measures','Nhận định · '+self.key,'Measure')},fontSize=b.lit(15),fontFamily=b.lit('Segoe UI Semibold'),fontColor=b.color(b.INK))
        self.block('insight_note',explanation,x,494,w,90,11,b.MUTED,'#FFF8ED',12)

    def flush(self):
        # Every page has its own supported facts, so all local slicers filter the entire page.
        self.doc['visualInteractions'] = [
            {'source': a['name'], 'target': z['name'],
             'type': 'DataFilter' if a['visual']['visualType'] == 'slicer' else 'NoFilter'}
            for a in self.visuals for z in self.visuals if a is not z]
        b.save(b.REPORT/f'definition/pages/{self.key}/page.json', self.doc)
        for v in self.visuals:
            b.save(b.REPORT/f'definition/pages/{self.key}/visuals/{v["name"]}/visual.json', v)


def top_filter(v, table, key, measure, count, exclusions=None):
    identity = b.field('d', key, alias=True)
    amount = b.field('m', measure, 'Measure', alias=True)
    ranked = {'Version': 2, 'From': [{'Name': 'd', 'Entity': table, 'Type': 0},
                                    {'Name': 'm', 'Entity': '_Measures', 'Type': 0}],
              'Select': [dict(identity, Name='field')],
              'Where': [{'Condition': {'Not': {'Expression': {'Comparison': {'ComparisonKind': 0,
                         'Left': amount, 'Right': {'Literal': {'Value': 'null'}}}}}}}],
              'OrderBy': [{'Direction': 2, 'Expression': amount}, {'Direction': 1, 'Expression': identity}], 'Top': count}
    if exclusions:
        ranked['Where'].extend(exclusions['filter']['Where'])
    v['filterConfig'] = {'filters': [{'name': v['name']+'top', 'field': b.field(table,key),
        'type': 'TopN', 'howCreated': 'User', 'filter': {'Version': 2,
        'From': [{'Name': 's', 'Expression': {'Subquery': {'Query': ranked}}, 'Type': 2},
                 {'Name': 'd', 'Entity': table, 'Type': 0}],
        'Where': [{'Condition': {'In': {'Expressions': [identity], 'Table': {'SourceRef': {'Source': 's'}}}}}]}}]}


def report_pages():
    root = (b.REPORT/'definition/pages').resolve()
    # Remove only the generated page tree after the entrypoint's backup and Desktop check.
    expected = (b.PROJECT/'ProcurementDashboard.Report/definition/pages').resolve()
    if root != expected or b.PROJECT.resolve() not in root.parents:
        raise RuntimeError('Unexpected generated page directory')
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    c, m = b.c, b.m
    month = c('dim_date', 'month_start')
    sector = c('dim_sector', 'sector_name')
    method = c('dim_procurement_method', 'method_category')
    geo = c('dim_geography', 'province_name')
    band = c('dim_value_band', 'band_label')
    pages = []

    p = Page('overview')
    p.filters(geography=True)
    p.cards(['Số gói kế hoạch', 'Kế hoạch · tỷ VND', 'Lượt kết quả hiệu lực', 'Trúng thầu · tỷ VND'])
    p.chart('trend', 'lineChart', month, ['Kế hoạch · tỷ VND', 'Trúng thầu · tỷ VND'], 228, 272, w=600, h=312, title='Giá trị kế hoạch và giá trúng theo tháng · tỷ VND')
    sector_colors = {'Hàng hóa': b.TEAL, 'Xây lắp': b.NAVY, 'Phi tư vấn': b.STEEL,
                     'Tư vấn': '#6283AE', 'Hỗn hợp': b.SAGE, 'Chưa xác định': b.NEUTRAL}
    slices = [
        {'properties': {'fill': b.color(fill)}, 'selector': {'data': [{'scopeId': {'Comparison': {
            'ComparisonKind': 0, 'Left': b.field(*sector[:2]), 'Right': b.lit(label)['expr']}}}]}}
        for label, fill in sector_colors.items()
    ]
    donut=p.add('sectors', 'donutChart', 842, 272, 434, 312, title='Cơ cấu giá trị trúng theo lĩnh vực',
          roles={'Category':[sector], 'Y':[m('Trúng thầu · tỷ VND')], 'Tooltips':[m('Mẫu giá trúng VND')]},
          objects={'legend':b.obj(show=b.lit(True), position=b.lit('Right'), showTitle=b.lit(False), fontSize=b.lit(10)),
                   'labels':b.obj(show=b.lit(True),labelStyle=b.lit('Percent of total'),fontSize=b.lit(10)), 'dataPoint': slices})
    donut['visual']['visualContainerObjects']['title'][0]['properties']['text']={'expr':b.field('_Measures','Tiêu đề cơ cấu lĩnh vực','Measure')}
    p.chart('bands', 'clusteredBarChart', band, ['Tỷ trọng giá kế hoạch VND', 'Tỷ trọng gói VND'], 228, 600, w=650, h=276, title='Quy mô gói · tỷ trọng giá trị và số gói VND')
    v = p.chart('geo', 'clusteredBarChart', c('dim_geography','province_display'), ['Trúng thầu · tỷ VND'], 892, 600, w=684, h=276, title='5 địa phương có giá trúng cao nhất · tỷ VND', tooltips=['Mẫu giá trúng VND'])
    excluded = b.categorical('dim_geography', 'geography_key', ['GEO_UNKNOWN', 'GEO_MULTI_UNALLOCATED'])
    predicate = excluded['filter']['Where'][0]
    predicate['Condition'] = {'Not': {'Expression': predicate['Condition']}}
    top_filter(v, 'dim_geography', 'geography_key', 'Trúng thầu · tỷ VND', 5, excluded)
    p.insight()
    pages.append(p)

    competition_tips = ['Lượt mở thầu','Lượt một nhà thầu','Lưu ý cỡ mẫu']
    p = Page('competition')
    p.filters(geography=True, method=True)
    p.cards(['Lượt mở thầu', 'Nhà thầu bình quân', 'Tỷ lệ một nhà thầu'])
    p.chart('trend', 'lineChart', month, ['Tỷ lệ một nhà thầu'], 228, 272, w=550, h=312, title='Tỷ lệ một nhà thầu theo tháng', tooltips=competition_tips)
    v = p.chart('sectorchart', 'clusteredBarChart', sector, ['Tỷ lệ một nhà thầu','Đối sánh tập chọn'], 792, 272, w=484, h=312, title='Một nhà thầu theo lĩnh vực · đối sánh gia quyền', tooltips=competition_tips)
    v['visual']['query']['sortDefinition']['sort'][0]['field']=b.field('_Measures','Lượt mở thầu','Measure')
    p.chart('methods', 'clusteredBarChart', method, ['Tỷ lệ một nhà thầu'], 228, 600, w=456, h=276, title='Một nhà thầu theo phương thức · % lượt mở', tooltips=competition_tips)
    p.matrix('bands', {'Rows':[band], 'Columns':[sector], 'Values':[m('Tỷ lệ một nhà thầu')], 'Tooltips':[m(n) for n in competition_tips]}, 'Một nhà thầu (%) · xanh: từ 30 lượt mở, kem: mẫu nhỏ', 698, 600, 878, 276, {'Tỷ lệ một nhà thầu': {'expr': b.field('_Measures', 'Màu cạnh tranh', 'Measure')}})
    p.insight()
    pages.append(p)

    p = Page('savings', currency=True)
    p.filters(geography=True, method=True)
    p.cards(['Dự toán mô tả · tỷ VND', 'Giá trúng mô tả · tỷ VND', 'Chênh lệch mô tả · tỷ VND', 'Tỷ lệ chênh lệch mô tả'])
    p.chart('trend', 'clusteredColumnChart', month, ['Chênh lệch mô tả · tỷ VND'], 850, 272, w=426, h=312, title='Chênh lệch mô tả theo tháng · tỷ VND', tooltips=['Mẫu mô tả'])
    p.chart('rates', 'lineChart', month, ['Tỷ lệ chênh lệch mô tả'], 228, 272, w=608, h=312, title='Tỷ lệ chênh lệch mô tả theo tháng', tooltips=['Mẫu mô tả', 'Độ phủ mẫu mô tả'])
    p.chart('sectors', 'clusteredBarChart', sector, ['Chênh lệch mô tả · tỷ VND'], 228, 600, w=650, h=276, title='Dự toán trừ giá trúng theo lĩnh vực · tỷ VND', tooltips=['Mẫu mô tả','Tỷ lệ chênh lệch mô tả'])
    p.chart('bands', 'hundredPercentStackedBarChart', band, ['Mẫu mô tả', 'Mẫu ngoài so sánh'], 892, 600, w=684, h=276, title='Điều kiện so sánh theo quy mô · % phạm vi', tooltips=['Phạm vi so sánh'])
    p.insight()
    pages.append(p)

    p = Page('duration')
    p.filters(method=True)
    labels = [b.LABELS[x] for x in SUPPORTED_METRICS]
    p.doc['filterConfig']['filters'].append(b.categorical('dim_duration_metric', 'metric_key', SUPPORTED_METRICS))
    p.slicer('metric', c('dim_duration_metric', 'metric_label'), 'Chỉ số · chọn một', 1008, 74, 274, 52, select=labels[0])
    p.cards(['Thời gian bình quân', 'Mẫu thời gian', 'Mẫu thời gian bị loại', 'Tỷ lệ mẫu bị loại'])
    p.chart('trend', 'lineChart', month, ['Thời gian bình quân'], 228, 272, w=604, h=312, title='Bình quân theo tháng · đơn vị trên card', tooltips=['Mẫu thời gian','Đơn vị thời gian'])
    p.chart('sectors', 'clusteredBarChart', sector, ['Thời gian bình quân'], 846, 272, w=430, h=312, title='Bình quân theo lĩnh vực', tooltips=['Mẫu thời gian','Đơn vị thời gian'])
    p.chart('methods', 'clusteredBarChart', method, ['Thời gian bình quân'], 228, 600, w=548, h=276, title='Thời gian bình quân theo phương thức', tooltips=['Đơn vị thời gian','Mẫu thời gian','Tỷ lệ mẫu bị loại'])
    p.chart('months', 'columnChart', month, ['Mẫu thời gian','Mẫu thời gian bị loại'], 790, 600, w=786, h=276, title='Mẫu hợp lệ và bị loại mỗi tháng · lượt', tooltips=['Tỷ lệ mẫu bị loại'])
    p.insight()
    pages.append(p)

    p = Page('suppliers', currency=True)
    p.filters(geography=True)
    p.cards(['Người nhận · tỷ VND', 'Số khóa người nhận', 'Độ phủ giá trị người nhận', 'Tỷ trọng Top 10 người nhận'])
    amount_name = 'Người nhận · tỷ VND'
    v = p.add('ranking', 'tableEx', 228, 272, 714, 530, title='10 người nhận có giá trị cao nhất · tỷ VND',
              roles={'Values':[m('Hạng người nhận'), m('Tên người nhận'), m(amount_name), c('dim_party','party_key')], 'Tooltips':[m('Khóa người nhận'), m('Mẫu giá người nhận')]})
    b.table_widths(v, [46, 311, 145, 164])
    v['visual']['objects']['grid']=b.obj(rowPadding=b.lit(5),gridVertical=b.lit(False))
    for proj,label in zip(v['visual']['query']['queryState']['Values']['projections'],['Hạng','Đơn vị nhận thầu','Tỷ VND','Định danh']):
        proj['displayName']=label
    v['visual']['objects']['columnFormatting'] = [{
        'selector': {'metadata':'_Measures.' + amount_name},
        'properties': {'dataBars': {'positiveColor': b.color('#94BCEF'), 'hideText': b.lit(False),
                                                  'negativeColor': b.color('#64748B'), 'axisColor': b.color(b.MUTED),
                                        'totalMatchingOption': {'expr': {'Literal': {'Value':'1L'}}},
                                        'reverseDirection': b.lit(False)}}}]
    v['visual']['query']['sortDefinition'] = {'sort':[{'field':b.field('_Measures', amount_name, 'Measure'), 'direction':'Descending'}]}
    top_filter(v, 'dim_party', 'party_key', 'Người nhận · tỷ VND', 10)
    p.chart('concentration', 'clusteredBarChart', sector, ['Tỷ trọng Top 10 người nhận'], 956, 272, w=320, h=312, title='Tỷ trọng Top 10 trong từng lĩnh vực', tooltips=['Số khóa người nhận', 'Người nhận · tỷ VND'])
    p.chart('allocation', 'clusteredBarChart', c('fact_supplier_daily','allocation_label'), ['Phân bổ · tỷ VND'], 956, 600, w=620, h=276, title='Giá trị theo trạng thái phân bổ · tỷ VND')
    p.insight()
    pages.append(p)

    p = Page('quality')
    p.cards(['Giá trúng thiếu địa phương','Giá chưa phân bổ'])
    streams = [n for n in b.MEASURES if n.startswith('Dòng · ')]
    p.matrix('coverage', {'Rows':[c('dim_date','year_month')], 'Values':[m(n) for n in streams]}, 'Quan sát theo tháng · xanh: có bản ghi, trống: chưa quan sát', 228, 272, 1048, 312, {n: {'expr': b.field('_Measures', 'Nền ' + n, 'Measure')} for n in streams})
    p.chart('coverage_rates', 'clusteredBarChart', sector, ['Độ phủ giá trúng','Độ phủ mẫu mô tả'], 228, 600, w=504, h=276, title='Độ phủ trong từng tập mẫu · %', tooltips=['Lượt kết quả hiệu lực','Phạm vi so sánh'])
    p.chart('samples', 'barChart', sector, ['Mẫu mô tả','Mẫu ngoài so sánh'], 746, 600, w=530, h=276, title='Phạm vi so sánh theo lĩnh vực · lượt', tooltips=['Độ phủ mẫu mô tả'])
    p.text('limits', 'GIỚI HẠN CẦN BIẾT\n\n• Có bản ghi chưa chứng minh thu thập đầy đủ.\n• Kỹ thuật và tiền dự thầu: cần xác minh cách tổng hợp.\n• Ô trống biểu thị thiếu quan sát hoặc thiếu mẫu hợp lệ.', 1290, 600, 286, 276, 12, b.INK)
    p.insight()
    pages.append(p)

    b.save(root/'pages.json', {'$schema': b.BASE+'item/report/definition/pagesMetadata/1.0.0/schema.json',
                             'pageOrder': [p.key for p in pages], 'activePageName': 'overview'})
    for p in pages:
        p.flush()
    b.save(b.PROJECT/'validation/report_inventory.json', {'pages': [{'name': p.key, 'displayName': p.title, 'visuals': len(p.visuals)} for p in pages]})
