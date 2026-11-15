"""Supported Gold V3 measures and pages; excluded source metrics stay out of the report."""
import json
import shutil

import build_project as b


PAGES = ['overview', 'demand', 'awards', 'savings', 'competition', 'duration', 'suppliers', 'quality']
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
        ('Lượt có giảm giá', 'fact_bidding_competition_daily', 'discounted_bidder_count'),
        ('Thông báo', 'fact_notice_daily', 'notice_count'),
        ('Thông báo hủy quan sát', 'fact_notice_daily', 'cancelled_notice_count'),
        ('Gói theo trạng thái quan sát', 'fact_status_daily', 'package_count'),
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
        ('Tỷ lệ lượt có giảm giá', 'DIVIDE([Lượt có giảm giá], [Lượt nhà thầu])', pct),
        ('Tỷ lệ hủy quan sát', 'DIVIDE([Thông báo hủy quan sát], [Thông báo])', pct),
    ]:
        add(name, dax, fmt, 'Tỷ lệ và bình quân')
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
    add('Lượt nhận đơn', f'CALCULATE(SUM(fact_supplier_daily[award_scope_count]), {supplier_filter})', folder='Người nhận')
    add('Phân bổ · tỷ VND', 'CALCULATE(IF(SUM(fact_supplier_daily[eligible_amount_count])>0, DIVIDE(SUM(fact_supplier_daily[award_amount_sum]),1000000000)), KEEPFILTERS(dim_currency[currency_code]="VND"))', money, 'Người nhận')
    add('Phân bổ · số lượt', 'SUM(fact_supplier_daily[award_scope_count])', folder='Người nhận')
    # Ranking removes only party identity, preserving the selected market and eligible population.
    add('Tỷ trọng Top 10 người nhận', '''VAR People = CALCULATETABLE(VALUES(dim_party[party_key]), REMOVEFILTERS(dim_party)) VAR Ranked = TOPN(10, ADDCOLUMNS(People, "_value", CALCULATE([Người nhận · tỷ VND])), [_value], DESC, dim_party[party_key], ASC) RETURN DIVIDE(SUMX(Ranked, [_value]), CALCULATE([Người nhận · tỷ VND], REMOVEFILTERS(dim_party)))''', pct, 'Người nhận')
    add('Nhóm kỹ thuật cần xác minh', 'COUNTROWS(FILTER(fact_bidding_competition_daily, fact_bidding_competition_daily[tech_passed_count]+fact_bidding_competition_daily[tech_failed_count]>fact_bidding_competition_daily[total_bidders]))', folder='Chất lượng')
    add('Nhóm tiền dự thầu lệch', 'COUNTROWS(FILTER(fact_bidding_competition_daily, ABS(fact_bidding_competition_daily[total_bid_amount]-fact_bidding_competition_daily[total_discount_amount]-fact_bidding_competition_daily[total_final_bid_amount])>0.01))', folder='Chất lượng', desc='Số nhóm lệch trên 0,01 đơn vị tiền nguồn sau chuyển double; không phải chênh lệch đã quy đổi VND.')
    add('Phạm vi cạnh tranh', 'VAR a=MIN(fact_bidding_competition_daily[report_date]) VAR z=MAX(fact_bidding_competition_daily[report_date]) RETURN IF(ISBLANK(a), "Không có dữ liệu trong phạm vi chọn", "Quan sát: " & FORMAT(a,"dd/MM/yyyy") & " – " & FORMAT(z,"dd/MM/yyyy"))', None, 'Chất lượng')
    for fact in ['demand', 'award', 'savings', 'bidding_competition', 'duration', 'supplier', 'notice', 'status']:
        add('Dòng · ' + fact, f'COUNTROWS(fact_{fact}_daily)', folder='Độ phủ', desc='Số nhóm tổng hợp có bản ghi; không chứng minh độ đầy đủ thu thập.')
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
        self.text('brand', 'MUA SẮM CÔNG VIỆT NAM  /  GOLD V3', 24, 12, 620, 20, 10, b.MUTED)
        self.text('heading', title, 24, 32, 640, 44, 23, b.NAVY)
        self.text('scope', subtitle, 712, 16, 704, 56, 10, b.MUTED)
        self.add('nav', 'pageNavigator', 24, 80, 1392, 40,
                 objects={'text': [{'properties': {'fontSize': b.lit(10)}, 'selector': {'id': 'default'}}]})
        date = self.slicer('date', b.c('dim_date', 'year_month'), 'Tháng · chọn một hoặc nhiều', 24, 132, 324, 56, sync='DateSync')
        self.slicer('sector', b.c('dim_sector', 'sector_name'), 'Lĩnh vực', 372, 132, 324, 56, sync='SectorSync')
        selected_months = [f'2025-{i:02}' for i in range(1, 13)]
        initial = b.categorical('dim_date', 'year_month', selected_months)['filter']
        date['visual']['objects']['general'] = b.obj(filter={'filter': initial})
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

    p = Page('overview', 'Tổng quan', 'Chỉ VND · mỗi chỉ tiêu theo ngày sự kiện riêng.\nMặc định 2025 · không suy ra tỷ lệ hoàn thành kế hoạch.', True)
    p.filters(geography=True)
    p.cards(['Kế hoạch · tỷ VND', 'Số gói kế hoạch', 'Trúng thầu · tỷ VND', 'Chênh lệch mô tả · tỷ VND'])
    p.chart('demandtrend', 'lineChart', month, ['Kế hoạch · tỷ VND'], 24, 308, title='Giá trị kế hoạch theo tháng · tỷ VND')
    p.chart('awardtrend', 'lineChart', month, ['Trúng thầu · tỷ VND'], 732, 308, title='Giá trị trúng thầu theo tháng · tỷ VND')
    p.chart('sectors', 'clusteredBarChart', sector, ['Trúng thầu · tỷ VND'], 24, 580, title='Giá trị trúng thầu theo lĩnh vực · tỷ VND')
    v = p.table('geos', [geo, m('Trúng thầu · tỷ VND')], 732, 580, 'Top 10 nhóm địa phương theo nguồn · tỷ VND', [400, 220], rank='Trúng thầu · tỷ VND')
    b.top10(v, 'dim_geography', 'province_name', 'fact_award_daily', 'award_amount_sum')
    p.foot('Chênh lệch là mô tả dự toán–giá trúng, chưa xác minh thuế/phạm vi. Không có mẫu tiết kiệm xác minh hoặc hợp đồng.')
    pages.append(p)

    p = Page('demand', 'Nhu cầu thị trường', 'Kế hoạch công bố theo ngày · chỉ VND.\nQuy mô là phân nhóm nguồn; không phải chi tiêu thực tế.', True)
    p.filters(geography=True, band=True)
    p.cards(['Số gói kế hoạch', 'Kế hoạch · tỷ VND', 'Kế hoạch bình quân · tỷ VND', 'Mẫu giá kế hoạch'])
    p.chart('trend', 'lineChart', month, ['Kế hoạch · tỷ VND'], 24, 308, title='Giá trị kế hoạch theo tháng · tỷ VND', tooltips=['Số gói kế hoạch'])
    v = p.chart('bands', 'clusteredColumnChart', band, ['Số gói kế hoạch'], 732, 308, title='Số gói kế hoạch theo quy mô', tooltips=['Kế hoạch · tỷ VND'])
    v['visual']['query']['sortDefinition'] = {'sort': [{'field': b.field('dim_value_band', 'band_label'), 'direction': 'Ascending'}], 'isDefaultSort': True}
    p.chart('sectors', 'clusteredBarChart', sector, ['Kế hoạch · tỷ VND'], 24, 580, title='Giá trị kế hoạch theo lĩnh vực · tỷ VND')
    p.table('geo', [geo, m('Số gói kế hoạch'), m('Kế hoạch · tỷ VND')], 732, 580, 'Địa phương · số gói và giá trị kế hoạch', [290, 150, 180], rank='Kế hoạch · tỷ VND')
    p.foot('Giữ nhóm chưa xác định và nhiều địa phương. Giá trị lớn là đối tượng rà soát, không tự loại khỏi tổng.')
    pages.append(p)

    p = Page('awards', 'Kết quả lựa chọn', 'Kết quả theo ngày công bố · chỉ VND.\nSố lượt kết quả không phải số nhà thầu duy nhất.', True)
    p.filters(geography=True, method=True)
    p.cards(['Lượt kết quả', 'Lượt kết quả hiệu lực', 'Trúng thầu · tỷ VND', 'Độ phủ giá trúng'])
    p.chart('trend', 'lineChart', month, ['Trúng thầu · tỷ VND'], 24, 308, title='Giá trị trúng thầu theo tháng · tỷ VND', tooltips=['Mẫu giá trúng'])
    p.chart('methods', 'clusteredBarChart', method, ['Trúng thầu · tỷ VND'], 732, 308, title='Giá trị theo phương thức · tỷ VND')
    p.chart('sectors', 'clusteredBarChart', sector, ['Lượt kết quả hiệu lực'], 24, 580, title='Lượt kết quả hiệu lực theo lĩnh vực')
    p.table('geo', [geo, m('Lượt kết quả hiệu lực'), m('Mẫu giá trúng'), m('Trúng thầu · tỷ VND')], 732, 580, 'Kết quả theo địa phương', [260, 120, 110, 140], rank='Trúng thầu · tỷ VND')
    p.foot('Độ phủ giá = mẫu giá hợp lệ / lượt hiệu lực. Không có chiều quy mô và không suy thành số hợp đồng.')
    pages.append(p)

    p = Page('savings', 'Chênh lệch mô tả', 'Dự toán và giá trúng trên cùng mẫu descriptive · VND.\nChưa phải tiết kiệm xác minh; giữ nguyên chênh lệch âm.', True)
    p.filters(geography=True, method=True)
    p.cards(['Dự toán mô tả · tỷ VND', 'Giá trúng mô tả · tỷ VND', 'Chênh lệch mô tả · tỷ VND', 'Tỷ lệ chênh lệch mô tả'])
    p.chart('trend', 'lineChart', month, ['Dự toán mô tả · tỷ VND', 'Giá trúng mô tả · tỷ VND'], 24, 308, title='Dự toán và giá trúng cùng mẫu · tỷ VND', tooltips=['Mẫu mô tả'])
    p.chart('rates', 'lineChart', month, ['Tỷ lệ chênh lệch mô tả'], 732, 308, title='Tỷ lệ chênh lệch mô tả theo tháng', tooltips=['Mẫu mô tả', 'Độ phủ mẫu mô tả'])
    p.table('sectors', [sector, m('Mẫu mô tả'), m('Chênh lệch mô tả · tỷ VND'), m('Tỷ lệ chênh lệch mô tả')], 24, 580, 'Chênh lệch theo lĩnh vực', [200, 110, 180, 145], rank='Chênh lệch mô tả · tỷ VND')
    p.table('bands', [band, m('Mẫu mô tả'), m('Độ phủ mẫu mô tả'), m('Chênh lệch mô tả · tỷ VND')], 732, 580, 'Mẫu và chênh lệch theo quy mô', [265, 100, 115, 150], rank='Chênh lệch mô tả · tỷ VND')
    p.foot('Tỷ lệ tính từ tổng cùng mẫu, không bình quân tỷ lệ. Verified và hợp đồng chưa có mẫu; không dựng KPI tiền bằng 0.')
    pages.append(p)

    p = Page('competition', 'Cạnh tranh cơ bản', 'Lượt tham gia, không phải nhà thầu duy nhất.\nLoại khỏi báo cáo KPI kỹ thuật và tiền dự thầu chưa xác minh.')
    p.filters(geography=True, method=True)
    p.cards(['Lượt mở thầu', 'Nhà thầu bình quân', 'Tỷ lệ một nhà thầu', 'Tỷ lệ lượt có giảm giá'])
    p.chart('trend', 'lineChart', month, ['Tỷ lệ một nhà thầu'], 24, 308, title='Tỷ lệ một nhà thầu theo tháng', tooltips=['Lượt mở thầu'])
    p.chart('sectorchart', 'clusteredColumnChart', sector, ['Lượt một nhà thầu', 'Lượt nhiều nhà thầu'], 732, 308, title='Lượt mở một / nhiều nhà thầu theo lĩnh vực')
    p.table('methods', [method, m('Lượt mở thầu'), m('Nhà thầu bình quân'), m('Tỷ lệ một nhà thầu')], 24, 580, 'Cạnh tranh theo phương thức', [285, 100, 120, 130], rank='Lượt mở thầu')
    p.table('bands', [band, m('Lượt mở thầu'), m('Nhà thầu bình quân'), m('Tỷ lệ một nhà thầu')], 732, 580, 'Cạnh tranh theo quy mô', [285, 100, 120, 130], rank='Lượt mở thầu')
    p.foot('Một nhà thầu là chỉ báo để rà soát, không kết luận sai phạm. Không gán tiền tệ VND cho các trường tiền cạnh tranh.')
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
    p.cards(['Người nhận · tỷ VND', 'Số khóa người nhận', 'Lượt nhận đơn', 'Tỷ trọng Top 10 người nhận'])
    v = p.table('ranking', [c('dim_party', 'party_display'), m('Lượt nhận đơn'), m('Người nhận · tỷ VND')], 24, 308, 'Top 10 khóa người nhận theo giá trị · tỷ VND', [470, 90, 170], w=790, h=520, rank='Người nhận · tỷ VND')
    b.top10(v, 'dim_party', 'party_display', 'fact_supplier_daily', 'award_amount_sum', True)
    v['filterConfig']['filters'].append(b.categorical('dim_party', 'party_kind', ['ORGANIZATION', 'CONSORTIUM', 'PERSON']))
    p.chart('trend', 'lineChart', month, ['Người nhận · tỷ VND'], 838, 308, w=578, title='Single recipient theo tháng · tỷ VND')
    p.table('allocation', [c('fact_supplier_daily', 'allocation_label'), m('Phân bổ · số lượt'), m('Phân bổ · tỷ VND')], 838, 580, 'Toàn bộ trạng thái phân bổ theo nguồn', [260, 110, 145], w=578)
    p.foot('Số khóa không đồng nghĩa số pháp nhân đã chuẩn hóa. Giữ party_key để phân biệt trùng tên; không có lọc phương thức hoặc quy mô.')
    pages.append(p)

    p = Page('quality', 'Chất lượng dữ liệu', 'Phạm vi sự kiện nguồn: 09/2022–12/2025.\nNgày phân tích: 15/11/2026; chưa có thời điểm xuất được chứng minh.')
    p.cards(['Mẫu mô tả', 'Mẫu xác minh', 'Mẫu hợp đồng', 'Nhóm kỹ thuật cần xác minh'])
    p.table('coverage', [c('dim_date', 'year_month')] + [m('Dòng · '+f) for f in ['demand', 'award', 'bidding_competition', 'duration']], 24, 308, 'Số nhóm có bản ghi theo tháng · không phải số giao dịch', [120, 120, 120, 135, 135])
    p.chart('status', 'clusteredBarChart', c('dim_progress_status', 'status_label'), ['Gói theo trạng thái quan sát'], 732, 308, title='Đã / chưa quan sát thông báo và kết quả')
    p.table('notices', [c('fact_notice_daily', 'notice_family'), m('Thông báo'), m('Thông báo hủy quan sát'), m('Tỷ lệ hủy quan sát')], 24, 580, 'Thông báo theo họ nguồn · hủy quan sát', [160, 140, 170, 160])
    p.text('limits', 'GIỚI HẠN CẦN BIẾT\n\n• Verified / hợp đồng: chưa có mẫu.\n• Kỹ thuật và tiền dự thầu: cần xác minh cách tổng hợp.\n• Hai mốc kết quả có dữ liệu trùng; bỏ mốc đầy đủ.\n• Không có hồ sơ gói thầu để tra cứu cá thể.\n• Lịch báo cáo giới hạn 2022–2025; năm 2022 chỉ từ tháng 9.\n• Trạng thái quan sát không phải tiến độ hợp đồng.', 748, 594, 652, 224, 12, b.INK)
    p.foot('PK / grain / FK đã kiểm tra toàn snapshot, không chứng minh đầy đủ nghiệp vụ. Tháng không có dòng không tự coi là hoạt động bằng 0.')
    pages.append(p)

    b.save(root/'pages.json', {'$schema': b.BASE+'item/report/definition/pagesMetadata/1.0.0/schema.json',
                             'pageOrder': [p.key for p in pages], 'activePageName': 'overview'})
    for p in pages:
        p.flush()
    b.save(b.PROJECT/'validation/report_inventory.json', {'pages': [{'name': p.key, 'displayName': p.title, 'visuals': len(p.visuals)} for p in pages]})
