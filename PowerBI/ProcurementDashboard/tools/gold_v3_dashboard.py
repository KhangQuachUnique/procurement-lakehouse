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
    # Cards use explicit Vietnamese formatting; numerical originals remain available to charts.
    for name, definition in list(b.MEASURES.items()):
        if definition['format'] is None:
            continue
        suffix = ' & " " & [Đơn vị thời gian]' if name == 'Thời gian bình quân' else ''
        add('Hiển thị · ' + name, f'IF(ISBLANK([{name}]), "—", FORMAT([{name}], "{definition["format"]}", "vi-VN"){suffix})', None, 'Hiển thị')


class Page(b.Page):
    def __init__(self, key, title, subtitle, currency=False):
        self.key = key
        self.title = title
        self.visuals = []
        self.metrics = []
        self.doc = {'$schema': b.BASE+'item/report/definition/page/2.0.0/schema.json',
                    'name': key, 'displayName': title, 'displayOption': 'FitToPage',
                    'height': 900, 'width': 1440,
                    'objects': {'background': b.obj(color=b.color(b.BG), transparency=b.lit(0))}}
        self.text('brand', 'MUA SẮM CÔNG VIỆT NAM  /  PHÂN TÍCH THỊ TRƯỜNG', 24, 12, 620, 20, 10, b.MUTED)
        self.text('heading', title, 24, 32, 640, 44, 23, b.NAVY)
        self.text('scope', subtitle, 712, 16, 704, 56, 10, b.MUTED)
        self.add('nav', 'pageNavigator', 24, 80, 1392, 40,
                 objects={'text': [{'properties': {'fontSize': b.lit(10)}, 'selector': {'id': 'default'}}]})
        date = self.slicer('date', b.c('dim_date', 'year_month'), 'Tháng · chọn một hoặc nhiều', 24, 132, 324, 56, sync='DateSync')
        self.slicer('sector', b.c('dim_sector', 'sector_name'), 'Lĩnh vực', 372, 132, 324, 56, sync='SectorSync')
        self.doc['filterConfig'] = {'filters': []}
        if currency:
            self.doc['filterConfig']['filters'].append(b.categorical('dim_currency', 'currency_code', ['VND']))

    def filters(self, geography=False, method=False, band=False):
        if geography:
            self.slicer('geography', b.c('dim_geography', 'province_name'), 'Địa phương theo nguồn', 720, 132, 324, 56)
        if method:
            self.slicer('method', b.c('dim_procurement_method', 'method_category'), 'Phương thức', 1068, 132, 348, 56)
        elif band:
            self.slicer('band', b.c('dim_value_band', 'band_label'), 'Quy mô gói', 1068, 132, 348, 56)

    def cards(self, names):
        for i, name in enumerate(names):
            self.card('k'+str(i), name, 24+i*354, y=204, w=330, h=84, size=22)

    def chart(self, key, typ, category, measures, x, y, w=684, h=248, title=None, tooltips=None):
        return super().chart(key, typ, category, measures, x, y, w, h, title, tooltips)

    def table(self, key, specs, x, y, title, widths, w=684, h=248, rank=None):
        v = self.add(key, 'tableEx', x, y, w, h, roles={'Values': specs}, title=title,
                     objects={'grid': b.obj(rowPadding=b.lit(3))})
        b.table_widths(v, widths)
        if rank:
            v['visual']['query']['sortDefinition'] = {'sort': [{'field': b.field('_Measures', rank, 'Measure'), 'direction': 'Descending'}], 'isDefaultSort': True}
        return v

    def foot(self, value):
        self.text('foot', value, 24, 850, 1392, 30, 10, b.MUTED)

    def flush(self):
        # Every page has its own supported facts, so all local slicers filter the entire page.
        self.doc['visualInteractions'] = [
            {'source': a['name'], 'target': z['name'],
             'type': 'DataFilter' if a['visual']['visualType'] == 'slicer' else 'NoFilter'}
            for a in self.visuals for z in self.visuals if a is not z]
        b.save(b.REPORT/f'definition/pages/{self.key}/page.json', self.doc)
        for v in self.visuals:
            b.save(b.REPORT/f'definition/pages/{self.key}/visuals/{v["name"]}/visual.json', v)


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

    p = Page('overview', 'Quy mô và cơ cấu thị trường', 'Số lượt gồm mọi tiền tệ. Giá trị tính trên mẫu VND có giá hợp lệ.')
    p.filters(geography=True)
    p.cards(['Số gói kế hoạch', 'Kế hoạch · tỷ VND', 'Lượt kết quả hiệu lực', 'Trúng thầu · tỷ VND'])
    p.chart('trend', 'lineChart', month, ['Kế hoạch · tỷ VND', 'Trúng thầu · tỷ VND'], 24, 308, title='Hai luồng công bố độc lập theo tháng · tỷ VND')
    p.chart('sectors', 'clusteredBarChart', sector, ['Trúng thầu · tỷ VND'], 732, 308, title='Giá trị trúng theo lĩnh vực · tỷ VND', tooltips=['Mẫu giá trúng VND'])
    p.table('bands', [band, m('Tỷ trọng gói VND'), m('Tỷ trọng giá kế hoạch VND')], 24, 580, 'Cơ cấu kế hoạch VND theo quy mô', [265, 160, 180], rank='Tỷ trọng giá kế hoạch VND')
    v = p.table('geo', [c('dim_geography', 'province_display'), m('Trúng thầu · tỷ VND')], 732, 580, 'Địa phương xác định theo giá trúng · tỷ VND', [445, 170], rank='Trúng thầu · tỷ VND')
    excluded = b.categorical('dim_geography', 'geography_key', ['GEO_UNKNOWN', 'GEO_MULTI_UNALLOCATED'])
    predicate = excluded['filter']['Where'][0]
    predicate['Condition'] = {'Not': {'Expression': predicate['Condition']}}
    v['filterConfig'] = {'filters': [excluded]}
    p.foot('Hai ngày công bố phản ánh hai tập sự kiện. Tổng thị trường bao gồm cả nhóm nhiều địa phương và chưa xác định.')
    pages.append(p)

    p = Page('competition', 'Cạnh tranh cơ bản', 'Lượt tham gia, không phải nhà thầu duy nhất.\nLoại khỏi báo cáo KPI kỹ thuật và tiền dự thầu chưa xác minh.')
    p.filters(geography=True, method=True)
    p.cards(['Lượt mở thầu', 'Nhà thầu bình quân', 'Tỷ lệ một nhà thầu'])
    p.chart('trend', 'lineChart', month, ['Tỷ lệ một nhà thầu'], 24, 308, title='Tỷ lệ một nhà thầu theo tháng', tooltips=['Lượt mở thầu'])
    p.table('sectorchart', [sector, m('Lượt mở thầu'), m('Tỷ lệ một nhà thầu'), m('Chênh so với tập chọn · đpt')], 732, 308, 'Một nhà thầu theo lĩnh vực · đối sánh với tập chọn', [175, 125, 150, 170], rank='Lượt mở thầu')
    p.table('methods', [method, m('Lượt mở thầu'), m('Nhà thầu bình quân'), m('Tỷ lệ một nhà thầu')], 24, 580, 'Cạnh tranh theo phương thức', [285, 100, 120, 130], rank='Lượt mở thầu')
    p.table('bands', [band, m('Lượt mở thầu'), m('Nhà thầu bình quân'), m('Tỷ lệ một nhà thầu')], 732, 580, 'Cạnh tranh theo quy mô', [285, 100, 120, 130], rank='Lượt mở thầu')
    p.foot('Một nhà thầu là chỉ báo để rà soát, không kết luận sai phạm. Không gán tiền tệ VND cho các trường tiền cạnh tranh.')
    pages.append(p)

    p = Page('savings', 'Chênh lệch mô tả', 'Dự toán và giá trúng trên cùng mẫu descriptive · VND.\nChưa phải tiết kiệm xác minh; giữ nguyên chênh lệch âm.', True)
    p.filters(geography=True, method=True)
    p.cards(['Dự toán mô tả · tỷ VND', 'Giá trúng mô tả · tỷ VND', 'Chênh lệch mô tả · tỷ VND', 'Tỷ lệ chênh lệch mô tả'])
    p.chart('trend', 'lineChart', month, ['Dự toán mô tả · tỷ VND', 'Giá trúng mô tả · tỷ VND'], 24, 308, title='Dự toán và giá trúng cùng mẫu · tỷ VND', tooltips=['Mẫu mô tả'])
    p.chart('rates', 'lineChart', month, ['Tỷ lệ chênh lệch mô tả'], 732, 308, title='Tỷ lệ chênh lệch mô tả theo tháng', tooltips=['Mẫu mô tả', 'Độ phủ mẫu mô tả'])
    p.table('sectors', [sector, m('Mẫu mô tả'), m('Chênh lệch mô tả · tỷ VND'), m('Tỷ lệ chênh lệch mô tả')], 24, 580, 'Chênh lệch theo lĩnh vực', [200, 110, 180, 145], rank='Chênh lệch mô tả · tỷ VND')
    p.table('bands', [band, m('Mẫu mô tả'), m('Độ phủ mẫu mô tả'), m('Chênh lệch mô tả · tỷ VND')], 732, 580, 'Mẫu và chênh lệch theo quy mô', [265, 100, 115, 150], rank='Chênh lệch mô tả · tỷ VND')
    p.foot('Tỷ lệ tính từ tổng cùng mẫu, không bình quân tỷ lệ. Độ phủ là số mẫu mô tả trên tổng phạm vi so sánh.')
    pages.append(p)

    p = Page('duration', 'Thời gian quy trình', 'Chỉ 5 metric có thể diễn giải theo nhãn nguồn.\nKhông dùng mốc đầy đủ trùng dữ liệu hoặc độ lệch kế hoạch chưa rõ dấu.')
    p.filters(method=True)
    labels = [b.LABELS[x] for x in SUPPORTED_METRICS]
    p.doc['filterConfig']['filters'].append(b.categorical('dim_duration_metric', 'metric_key', SUPPORTED_METRICS))
    p.slicer('metric', c('dim_duration_metric', 'metric_label'), 'Chỉ số · chọn một', 720, 132, 324, 56, select=labels[0])
    p.cards(['Thời gian bình quân', 'Mẫu thời gian', 'Mẫu thời gian bị loại', 'Tỷ lệ mẫu bị loại'])
    p.chart('trend', 'lineChart', month, ['Thời gian bình quân'], 24, 308, title='Bình quân theo tháng · đơn vị trên card', tooltips=['Mẫu thời gian'])
    p.chart('sectors', 'clusteredBarChart', sector, ['Thời gian bình quân'], 732, 308, title='Bình quân theo lĩnh vực', tooltips=['Mẫu thời gian'])
    p.table('methods', [method, m('Thời gian bình quân'), m('Mẫu thời gian'), m('Tỷ lệ mẫu bị loại')], 24, 580, 'Thời gian theo phương thức', [285, 120, 110, 120], rank='Mẫu thời gian')
    p.table('months', [c('dim_date', 'year_month'), m('Thời gian bình quân'), m('Mẫu thời gian'), m('Mẫu thời gian bị loại')], 732, 580, 'Mẫu hợp lệ và bị loại theo tháng', [150, 140, 170, 170])
    p.foot('Bình quân = tổng duration_sum / tổng sample_count của đúng một metric. Không cộng metric; không suy ra P90 hoặc quá hạn.')
    pages.append(p)

    p = Page('suppliers', 'Đơn vị nhận thầu', 'Xếp hạng single_recipient, VND, có mẫu tiền hợp lệ.\nKhông chia đều giá trị liên danh hoặc nhiều người nhận.', True)
    p.filters(geography=True)
    p.cards(['Người nhận · tỷ VND', 'Số khóa người nhận', 'Độ phủ giá trị người nhận', 'Tỷ trọng Top 10 người nhận'])
    v = p.table('ranking', [c('dim_party', 'party_display'), m('Mẫu giá người nhận'), m('Người nhận · tỷ VND')], 24, 308, 'Top 10 khóa người nhận theo giá trị · tỷ VND', [470, 90, 170], w=790, h=520, rank='Người nhận · tỷ VND')
    # Use the same eligibility measure and stable identity as CR10.
    identity = b.field('d', 'party_key', alias=True)
    amount = b.field('m', 'Người nhận · tỷ VND', 'Measure', alias=True)
    ranked = {'Version': 2, 'From': [{'Name': 'd', 'Entity': 'dim_party', 'Type': 0},
                                    {'Name': 'm', 'Entity': '_Measures', 'Type': 0}],
              'Select': [dict(identity, Name='field')],
              'Where': [{'Condition': {'Not': {'Expression': {'Comparison': {'ComparisonKind': 0,
                         'Left': amount, 'Right': {'Literal': {'Value': 'null'}}}}}}}],
              'OrderBy': [{'Direction': 2, 'Expression': amount}, {'Direction': 1, 'Expression': identity}], 'Top': 10}
    v['filterConfig'] = {'filters': [{'name': v['name']+'top', 'field': b.field('dim_party','party_key'),
        'type': 'TopN', 'howCreated': 'User', 'filter': {'Version': 2,
        'From': [{'Name': 's', 'Expression': {'Subquery': {'Query': ranked}}, 'Type': 2},
                 {'Name': 'd', 'Entity': 'dim_party', 'Type': 0}],
        'Where': [{'Condition': {'In': {'Expressions': [identity], 'Table': {'SourceRef': {'Source': 's'}}}}}]}}]}
    p.chart('concentration', 'clusteredBarChart', sector, ['Tỷ trọng Top 10 người nhận'], 838, 308, w=578, title='Tỷ trọng Top 10 trong từng lĩnh vực', tooltips=['Số khóa người nhận', 'Người nhận · tỷ VND'])
    p.table('allocation', [c('fact_supplier_daily', 'allocation_label'), m('Phân bổ · số lượt'), m('Phân bổ · tỷ VND')], 838, 580, 'Toàn bộ trạng thái phân bổ theo nguồn', [260, 110, 145], w=578)
    p.foot('Số khóa không đồng nghĩa số pháp nhân đã chuẩn hóa. Giữ party_key để phân biệt trùng tên; không có lọc phương thức hoặc quy mô.')
    pages.append(p)

    p = Page('quality', 'Chất lượng dữ liệu', 'Phạm vi theo tháng và lĩnh vực đang chọn. Ngày sự kiện phụ thuộc từng fact.')
    p.cards(['Mẫu mô tả', 'Mẫu xác minh', 'Mẫu hợp đồng', 'Độ phủ mẫu mô tả'])
    p.table('coverage', [c('dim_date', 'year_month'), m('Luồng có quan sát')], 24, 308, 'Luồng sự kiện có quan sát theo tháng', [112, 520])
    p.chart('status', 'clusteredBarChart', c('dim_progress_status', 'status_label'), ['Gói theo trạng thái quan sát'], 732, 308, title='Trạng thái tại ngày cuối có bản ghi trong tập chọn')
    p.table('notices', [c('fact_notice_daily', 'notice_family'), m('Thông báo'), m('Thông báo hủy quan sát'), m('Tỷ lệ hủy quan sát')], 24, 580, 'Thông báo theo họ nguồn · hủy quan sát', [160, 140, 170, 160])
    p.text('limits', 'GIỚI HẠN CẦN BIẾT\n\n• Xem cỡ mẫu xác minh và hợp đồng ở trên.\n• Kỹ thuật và tiền dự thầu: cần xác minh cách tổng hợp.\n• Hai mốc kết quả có dữ liệu trùng; bỏ mốc đầy đủ.\n• Không có hồ sơ gói thầu để tra cứu cá thể.\n• Tháng thiếu quan sát hiển thị trống.\n• Trạng thái quan sát không phải tiến độ hợp đồng.', 748, 594, 652, 224, 12, b.INK)
    p.foot('Ô trống cho biết thiếu quan sát hoặc thiếu mẫu đủ điều kiện. Các tỷ lệ chỉ đại diện cho phạm vi được ghi nhận.')
    pages.append(p)

    b.save(root/'pages.json', {'$schema': b.BASE+'item/report/definition/pagesMetadata/1.0.0/schema.json',
                             'pageOrder': [p.key for p in pages], 'activePageName': 'overview'})
    for p in pages:
        p.flush()
    b.save(b.PROJECT/'validation/report_inventory.json', {'pages': [{'name': p.key, 'displayName': p.title, 'visuals': len(p.visuals)} for p in pages]})
