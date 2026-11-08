"""Source-shaped business fixture, preserved from reviewed legacy tests; no legacy runtime."""

import duckdb


def populate_comprehensive_silver_fixtures(con: duckdb.DuckDBPyConnection) -> None:
    """Populate test silver tables with adversarial source-shaped fixtures covering all scenarios."""
    # Observations
    con.execute("""
        INSERT INTO silver.observation (
            observation_id, resource, source_date, run_id, table_name,
            file_key, file_sha256, row_ordinal, observed_at,
            source_id, source_version, content_hash, payload_json
        ) VALUES
        ('obs_p1', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k1', 'sha1', 1, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p1', '1', 'ch1', '{}'),
        ('obs_p2', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k2', 'sha2', 2, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p2', '1', 'ch2', '{}'),
        ('obs_p3', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k3', 'sha3', 3, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p3', '1', 'ch3', '{}'),
        ('obs_p4', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k4', 'sha4', 4, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p4', '1', 'ch4', '{}'),
        ('obs_p5', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k5', 'sha5', 5, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p5', '1', 'ch5', '{}'),
        ('obs_p6a', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k6a', 'sha6a', 6, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p6a', '1', 'ch6a', '{}'),
        ('obs_p6b', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k6b', 'sha6b', 7, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p6b', '1', 'ch6b', '{}'),
        ('obs_p7', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k7', 'sha7', 8, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p7', '1', 'ch7', '{}'),
        ('obs_p8', 'khlcnt', DATE '2020-05-01', 'run1', 'package', 'k8', 'sha8', 9, TIMESTAMPTZ '2020-05-01 08:00:00+00', 'p8', '1', 'ch8', '{}'),
        ('obs_p9', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k9', 'sha9', 10, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p9', '1', 'ch9', '{}'),
        ('obs_p10', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k10', 'sha10', 11, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p10', '1', 'ch10', '{}'),
        ('obs_p11', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k11', 'sha11', 12, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p11', '1', 'ch11', '{}'),
        ('obs_p12_v2', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k12a', 'sha12a', 13, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p12', '2', 'ch12a', '{}'),
        ('obs_p12_v10', 'khlcnt', DATE '2024-01-02', 'run1', 'package', 'k12b', 'sha12b', 14, TIMESTAMPTZ '2024-01-02 08:00:00+00', 'p12', '10', 'ch12b', '{}'),
        ('obs_p13a', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k13a', 'sha13a', 15, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p13a', '1', 'ch13a', '{}'),
        ('obs_p13b', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k13b', 'sha13b', 16, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p13b', '1', 'ch13b', '{}'),
        ('obs_p14', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k14', 'sha14', 17, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p14', '1', 'ch14', '{}'),
        ('obs_p15', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k15', 'sha15', 18, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p15', '1', 'ch15', '{}'),
        ('obs_p16', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k16', 'sha16', 19, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p16', '1', 'ch16', '{}'),
        ('obs_p17', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k17', 'sha17', 20, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p17', '1', 'ch17', '{}'),
        ('obs_p18', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k18', 'sha18', 21, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p18', '1', 'ch18', '{}'),
        ('obs_p19', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k19', 'sha19', 22, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p19', '1', 'ch19', '{}'),
        ('obs_p20', 'khlcnt', DATE '2024-01-01', 'run1', 'package', 'k20', 'sha20', 23, TIMESTAMPTZ '2024-01-01 08:00:00+00', 'p20', '1', 'ch20', '{}');
    """)

    # Plans
    con.execute("""
        INSERT INTO silver.plan_revision (
            revision_id, entity_id, entity_type, observation_id, is_current,
            plan_no, title, investor_name, project_id, project_no,
            public_date, root_json
        ) VALUES
        ('plan_rev_1', 'plan1', 'plan', 'obs_p1', TRUE, 'PL_001', 'KHLCNT Du An 1', 'Ban QLDA Giao Thong', 'proj1', 'PRJ_001', DATE '2023-12-01', '{}');
    """)

    # Packages (F1 Candidates)
    con.execute("""
        INSERT INTO silver.package_revision (
            revision_id, entity_id, entity_type, observation_id, is_current,
            package_no, title, plan_no, plan_id, bid_price, currency,
            bid_field, bid_form, bid_mode, bid_start_year, bid_start_month,
            public_date, root_json
        ) VALUES
        -- Scenario 1: Multi-lot Package P1 (100M VND total baseline)
        ('rev_p1', 'p1', 'package', 'obs_p1', TRUE, 'PKG_001', 'Goi thau 1 gom 2 lo', 'PL_001', 'plan1', 100000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 2, DATE '2024-01-05', '{"id": "p1"}'),
        -- Scenario 2: Same-price independent winners Package P2
        ('rev_p2', 'p2', 'package', 'obs_p2', TRUE, 'PKG_002', 'Goi thau 2 nha thau cung gia', 'PL_001', 'plan1', 120000000.0, 'VND', 'HH', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 3, DATE '2024-01-10', '{"id": "p2"}'),
        -- Scenario 3: Consortium Package P3
        ('rev_p3', 'p3', 'package', 'obs_p3', TRUE, 'PKG_003', 'Goi thau 3 lien danh', 'PL_001', 'plan1', 100000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 4, DATE '2024-01-15', '{"id": "p3"}'),
        -- Scenario 4: Multi-location Package P4
        ('rev_p4', 'p4', 'package', 'obs_p4', TRUE, 'PKG_004', 'Goi thau 4 da dia ban', 'PL_001', 'plan1', 80000000.0, 'VND', 'PTV', 'CHCT', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 5, DATE '2024-01-20', '{"id": "p4"}'),
        -- Scenario 5: Missing notice direct package P5
        ('rev_p5', 'p5', 'package', 'obs_p5', TRUE, 'PKG_005', 'Goi thau 5 khong co TBMT', 'PL_001', 'plan1', 50000000.0, 'VND', 'TV', 'CDT', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 6, DATE '2024-01-25', '{"id": "p5"}'),
        -- Scenario 6: Ambiguous duplicated package_no
        ('rev_p6a', 'p6a', 'package', 'obs_p6a', TRUE, 'PKG_DUP_NO', 'Goi thau 6A trung ma', 'PL_001', 'plan1', 30000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 7, DATE '2024-02-01', '{"id": "p6a"}'),
        ('rev_p6b', 'p6b', 'package', 'obs_p6b', TRUE, 'PKG_DUP_NO', 'Goi thau 6B trung ma', 'PL_001', 'plan1', 35000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 7, DATE '2024-02-01', '{"id": "p6b"}'),
        -- Scenario 7: Cancelled package P7
        ('rev_p7', 'p7', 'package', 'obs_p7', TRUE, 'PKG_007', 'Goi thau 7 bi huy', 'PL_001', 'plan1', 40000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 8, DATE '2024-02-10', '{"id": "p7"}'),
        -- Scenario 8: Baseline outside event period P8 (2020)
        ('rev_p8', 'p8', 'package', 'obs_p8', TRUE, 'PKG_008', 'Goi thau 8 gia co so nam 2020', 'PL_001', 'plan1', 90000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2020, 5, DATE '2020-05-01', '{"id": "p8"}'),
        -- Scenario 9: Result outside event period P9 (2026 result)
        ('rev_p9', 'p9', 'package', 'obs_p9', TRUE, 'PKG_009', 'Goi thau 9 ket qua nam 2026', 'PL_001', 'plan1', 70000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 9, DATE '2024-03-01', '{"id": "p9"}'),
        -- Scenario 10: Unknown currency P10
        ('rev_p10', 'p10', 'package', 'obs_p10', TRUE, 'PKG_010', 'Goi thau 10 khong ro tien te', 'PL_001', 'plan1', 60000000.0, NULL, 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 10, DATE '2024-03-05', '{"id": "p10"}'),
        -- Scenario 11: Negative savings P11 (award 110M > baseline 100M)
        ('rev_p11', 'p11', 'package', 'obs_p11', TRUE, 'PKG_011', 'Goi thau 11 tiet kiem am', 'PL_001', 'plan1', 100000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 11, DATE '2024-03-10', '{"id": "p11"}'),
        -- Scenario 12: Source revisions 2 vs 10 P12
        ('rev_p12_v2', 'p12', 'package', 'obs_p12_v2', FALSE, 'PKG_012', 'Goi thau 12 ban 2 gia re', 'PL_001', 'plan1', 20000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 1, DATE '2024-01-01', '{"id": "p12"}'),
        ('rev_p12_v10', 'p12', 'package', 'obs_p12_v10', TRUE, 'PKG_012', 'Goi thau 12 ban 10 chuan', 'PL_001', 'plan1', 50000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 1, DATE '2024-01-02', '{"id": "p12"}'),
        -- Scenario 13: Two packages sharing lot number (P13A and P13B)
        ('rev_p13a', 'p13a', 'package', 'obs_p13a', TRUE, 'PKG_013A', 'Goi thau 13A co lo 1', 'PL_001', 'plan1', 40000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 1, DATE '2024-01-05', '{"id": "p13a"}'),
        ('rev_p13b', 'p13b', 'package', 'obs_p13b', TRUE, 'PKG_013B', 'Goi thau 13B cung co lo 1', 'PL_001', 'plan1', 60000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 1, DATE '2024-01-05', '{"id": "p13b"}'),
        -- Scenario 14: Repeated lot numbers with different source IDs (P14)
        ('rev_p14', 'p14', 'package', 'obs_p14', TRUE, 'PKG_014', 'Goi thau 14 co 2 lo cung so 1', 'PL_001', 'plan1', 70000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 1, DATE '2024-01-05', '{"id": "p14"}'),
        -- Scenario 15: SHA-256 hashed entity ID with raw UUID in root_json
        ('rev_p15', 'a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890', 'package', 'obs_p15', TRUE, 'PKG_015', 'Goi thau 15 SHA ID', 'PL_001', 'plan1', 50000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 1, DATE '2024-01-05', '{"id": "uuid-raw-pkg-15"}'),
        -- Scenario 16: Unknown status with positive money
        ('rev_p16', 'p16', 'package', 'obs_p16', TRUE, 'PKG_016', 'Goi thau 16 trang thai khong xac dinh', 'PL_001', 'plan1', 50000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 1, DATE '2024-01-05', '{"id": "p16"}'),
        -- Scenario 17: Baseline after award
        ('rev_p17', 'p17', 'package', 'obs_p17', TRUE, 'PKG_017', 'Goi thau 17 baseline sau trao thau', 'PL_001', 'plan1', 50000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 8, DATE '2024-08-01', '{"id": "p17"}'),
        -- Scenario 18: USD Currency Package
        ('rev_p18', 'p18', 'package', 'obs_p18', TRUE, 'PKG_018', 'Goi thau 18 USD', 'PL_001', 'plan1', 200000.0, 'USD', 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 1, DATE '2024-01-05', '{"id": "p18"}'),
        -- Scenario 19: Unknown real geography code (TTH)
        ('rev_p19', 'p19', 'package', 'obs_p19', TRUE, 'PKG_019', 'Goi thau 19 Thua Thien Hue', 'PL_001', 'plan1', 30000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 1, DATE '2024-01-05', '{"id": "p19"}'),
        -- Scenario 20: Package with notice but NO result
        ('rev_p20', 'p20', 'package', 'obs_p20', TRUE, 'PKG_020', 'Goi thau 20 khong co ket qua', 'PL_001', 'plan1', 30000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN_1_TUI_HO_SO', 2024, 1, DATE '2024-01-05', '{"id": "p20"}');
    """)

    # Package locations
    con.execute("""
        INSERT INTO silver.package_location (
            location_id, revision_id, location_ordinal, province_code, province_name
        ) VALUES
        ('loc1', 'rev_p1', 1, 'HNI', 'Hà Nội'),
        ('loc2', 'rev_p2', 1, 'HCM', 'Hồ Chí Minh'),
        ('loc3', 'rev_p3', 1, 'DNG', 'Đà Nẵng'),
        ('loc4a', 'rev_p4', 1, 'HNI', 'Hà Nội'),
        ('loc4b', 'rev_p4', 2, 'DNG', 'Đà Nẵng'),
        ('loc5', 'rev_p5', 1, 'HNI', 'Hà Nội'),
        ('loc11', 'rev_p11', 1, 'HNI', 'Hà Nội'),
        ('loc19', 'rev_p19', 1, 'TTH', 'Thừa Thiên Huế');
    """)

    # Package lots for P1 (Lot 1: 60M, Lot 2: 40M)
    con.execute("""
        INSERT INTO silver.package_lot (
            lot_id, revision_id, source_lot_id, lot_ordinal, lot_no, lot_name, lot_price, currency
        ) VALUES
        ('plot_1_1', 'rev_p1', 'slot1', 1, '1', 'Lo so 1', 60000000.0, 'VND'),
        ('plot_1_2', 'rev_p1', 'slot2', 2, '2', 'Lo so 2', 40000000.0, 'VND');
    """)

    # Notices (F2 Candidates)
    con.execute("""
        INSERT INTO silver.notice_revision (
            revision_id, entity_id, entity_type, observation_id, is_current,
            notify_no, notify_version, package_id, package_no, title,
            procuring_entity_code, procuring_entity_name, investor_name,
            bid_open_date, bid_close_date, bid_price, currency,
            bid_field, bid_form, bid_mode, published_at, public_date,
            guarantee_amount, guarantee_currency, guarantee_form,
            status_raw, cancel_reason, root_json
        ) VALUES
        ('rev_not_1', 'not1', 'notice', 'obs_p1', TRUE, 'TBMT_001', '00', 'p1', 'PKG_001', 'TBMT Goi 1', 'PE_01', 'Ben moi thau 1', 'Chu dau tu 1', TIMESTAMPTZ '2024-01-10 08:00:00+00', TIMESTAMPTZ '2024-01-20 08:00:00+00', 100000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN', TIMESTAMPTZ '2024-01-10 08:00:00+00', DATE '2024-01-10', 1000000.0, 'VND', 'Thu bao lanh', 'IS_PUBLISH', NULL, '{}'),
        ('rev_not_2', 'not2', 'notice', 'obs_p2', TRUE, 'TBMT_002', '00', 'p2', 'PKG_002', 'TBMT Goi 2', 'PE_01', 'Ben moi thau 1', 'Chu dau tu 1', TIMESTAMPTZ '2024-01-15 08:00:00+00', TIMESTAMPTZ '2024-01-25 08:00:00+00', 120000000.0, 'VND', 'HH', 'DTR', '1_GIAI_DOAN', TIMESTAMPTZ '2024-01-15 08:00:00+00', DATE '2024-01-15', 1200000.0, 'VND', 'Thu bao lanh', 'IS_PUBLISH', NULL, '{}'),
        ('rev_not_3', 'not3', 'notice', 'obs_p3', TRUE, 'TBMT_003', '00', 'p3', 'PKG_003', 'TBMT Goi 3', 'PE_01', 'Ben moi thau 1', 'Chu dau tu 1', TIMESTAMPTZ '2024-01-20 08:00:00+00', TIMESTAMPTZ '2024-01-30 08:00:00+00', 100000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN', TIMESTAMPTZ '2024-01-20 08:00:00+00', DATE '2024-01-20', 1000000.0, 'VND', 'Thu bao lanh', 'IS_PUBLISH', NULL, '{}'),
        -- Cancelled notice
        ('rev_not_7', 'not7', 'notice', 'obs_p7', TRUE, 'TBMT_007', '00', 'p7', 'PKG_007', 'TBMT Goi 7 bi huy', 'PE_01', 'Ben moi thau 1', 'Chu dau tu 1', TIMESTAMPTZ '2024-02-15 08:00:00+00', TIMESTAMPTZ '2024-02-25 08:00:00+00', 40000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN', TIMESTAMPTZ '2024-02-15 08:00:00+00', DATE '2024-02-15', 400000.0, 'VND', 'Thu bao lanh', 'ĐÃ HỦY THẦU', 'Huy thau theo quyet dinh', '{}'),
        -- Notice with no result (P20)
        ('rev_not_20', 'not20', 'notice', 'obs_p20', TRUE, 'TBMT_020', '00', 'p20', 'PKG_020', 'TBMT Goi 20', 'PE_01', 'Ben moi thau 1', 'Chu dau tu 1', TIMESTAMPTZ '2024-01-10 08:00:00+00', TIMESTAMPTZ '2024-01-20 08:00:00+00', 30000000.0, 'VND', 'XL', 'DTR', '1_GIAI_DOAN', TIMESTAMPTZ '2024-01-10 08:00:00+00', DATE '2024-01-10', 300000.0, 'VND', 'Thu bao lanh', 'IS_PUBLISH', NULL, '{}');
    """)

    # Results (F3 Candidates)
    con.execute("""
        INSERT INTO silver.result_revision (
            revision_id, entity_id, entity_type, observation_id, is_current,
            result_id, result_version, notify_no, notify_version,
            bid_id, package_no, title, decision_no, decision_date,
            total_winning_price, amount, currency, public_date, status_raw, root_json
        ) VALUES
        -- Scenario 1: Result 1 for P1 (2 lots in 2024 and 2025)
        ('rev_res_1_2024', 'res1_2024', 'result', 'obs_p1', TRUE, 'RES_001_2024', '00', 'TBMT_001', '00', 'p1', 'PKG_001', 'KQLCNT Goi 1 Lo 1', 'QD_01', DATE '2024-06-01', 54000000.0, 60000000.0, 'VND', DATE '2024-06-01', 'IS_PUBLISH', '{}'),
        ('rev_res_1_2025', 'res1_2025', 'result', 'obs_p1', TRUE, 'RES_001_2025', '00', 'TBMT_001', '00', 'p1', 'PKG_001', 'KQLCNT Goi 1 Lo 2', 'QD_02', DATE '2025-02-15', 38000000.0, 40000000.0, 'VND', DATE '2025-02-15', 'IS_PUBLISH', '{}'),
        -- Scenario 2: Result 2 for P2 (2 independent winners each 50M -> total 100M)
        ('rev_res_2', 'res2', 'result', 'obs_p2', TRUE, 'RES_002', '00', 'TBMT_002', '00', 'p2', 'PKG_002', 'KQLCNT Goi 2', 'QD_03', DATE '2024-07-01', 100000000.0, 120000000.0, 'VND', DATE '2024-07-01', 'IS_PUBLISH', '{}'),
        -- Scenario 3: Result 3 for P3 (Consortium award 90M)
        ('rev_res_3', 'res3', 'result', 'obs_p3', TRUE, 'RES_003', '00', 'TBMT_003', '00', 'p3', 'PKG_003', 'KQLCNT Goi 3', 'QD_04', DATE '2024-08-01', 90000000.0, 100000000.0, 'VND', DATE '2024-08-01', 'IS_PUBLISH', '{}'),
        -- Scenario 5: Missing notice direct package P5 (award 45M vs baseline 50M)
        ('rev_res_5', 'res5', 'result', 'obs_p5', TRUE, 'RES_005', '00', NULL, NULL, 'p5', 'PKG_005', 'KQLCNT Goi 5', 'QD_05', DATE '2024-09-01', 45000000.0, 50000000.0, 'VND', DATE '2024-09-01', 'IS_PUBLISH', '{}'),
        -- Scenario 6: Ambiguous business code (matches PKG_DUP_NO)
        ('rev_res_6', 'res6', 'result', 'obs_p6a', TRUE, 'RES_006', '00', NULL, NULL, 'non_existent_bid_id', 'PKG_DUP_NO', 'KQLCNT Trung Ma', 'QD_06', DATE '2024-09-10', 28000000.0, 30000000.0, 'VND', DATE '2024-09-10', 'IS_PUBLISH', '{}'),
        -- Scenario 7: Cancelled award result
        ('rev_res_7', 'res7', 'result', 'obs_p7', TRUE, 'RES_007', '00', 'TBMT_007', '00', 'p7', 'PKG_007', 'KQLCNT Goi 7 Bi Huy', 'QD_07', DATE '2024-09-15', 35000000.0, 40000000.0, 'VND', DATE '2024-09-15', 'CANCELLED', '{"cancelReason": "Huy theo quyet dinh QD_07"}'),
        -- Scenario 8: Baseline 2020 (P8) award in 2024
        ('rev_res_8', 'res8', 'result', 'obs_p8', TRUE, 'RES_008', '00', NULL, NULL, 'p8', 'PKG_008', 'KQLCNT Goi 8', 'QD_08', DATE '2024-10-01', 80000000.0, 90000000.0, 'VND', DATE '2024-10-01', 'IS_PUBLISH', '{}'),
        -- Scenario 9: Result in 2026 (outside 2022-2025 event period)
        ('rev_res_9', 'res9', 'result', 'obs_p9', TRUE, 'RES_009', '00', NULL, NULL, 'p9', 'PKG_009', 'KQLCNT Goi 9 2026', 'QD_09', DATE '2026-03-01', 65000000.0, 70000000.0, 'VND', DATE '2026-03-01', 'IS_PUBLISH', '{}'),
        -- Scenario 10: Unknown currency result
        ('rev_res_10', 'res10', 'result', 'obs_p10', TRUE, 'RES_010', '00', NULL, NULL, 'p10', 'PKG_010', 'KQLCNT Goi 10', 'QD_10', DATE '2024-11-01', 55000000.0, 60000000.0, 'UNKNOWN', DATE '2024-11-01', 'IS_PUBLISH', '{}'),
        -- Scenario 11: Negative savings (award 110M vs baseline 100M)
        ('rev_res_11', 'res11', 'result', 'obs_p11', TRUE, 'RES_011', '00', NULL, NULL, 'p11', 'PKG_011', 'KQLCNT Goi 11 Tiet Kiem Am', 'QD_11', DATE '2024-12-01', 110000000.0, 100000000.0, 'VND', DATE '2024-12-01', 'IS_PUBLISH', '{}'),
        -- Scenario 13: Result for P13A and P13B sharing lot number 1
        ('rev_res_13a', 'res13a', 'result', 'obs_p13a', TRUE, 'RES_013A', '00', NULL, NULL, 'p13a', 'PKG_013A', 'KQLCNT Goi 13A', 'QD_13A', DATE '2024-05-01', 36000000.0, 40000000.0, 'VND', DATE '2024-05-01', 'IS_PUBLISH', '{}'),
        ('rev_res_13b', 'res13b', 'result', 'obs_p13b', TRUE, 'RES_013B', '00', NULL, NULL, 'p13b', 'PKG_013B', 'KQLCNT Goi 13B', 'QD_13B', DATE '2024-05-01', 54000000.0, 60000000.0, 'VND', DATE '2024-05-01', 'IS_PUBLISH', '{}'),
        -- Scenario 14: Result for P14 with 2 lots both having lot_no = '1'
        ('rev_res_14', 'res14', 'result', 'obs_p14', TRUE, 'RES_014', '00', NULL, NULL, 'p14', 'PKG_014', 'KQLCNT Goi 14', 'QD_14', DATE '2024-05-15', 65000000.0, 70000000.0, 'VND', DATE '2024-05-15', 'IS_PUBLISH', '{}'),
        -- Scenario 15: Result matching package via root_json.id (raw UUID, missing package_no)
        ('rev_res_15', 'res15', 'result', 'obs_p15', TRUE, 'RES_015', '00', NULL, NULL, 'uuid-raw-pkg-15', NULL, 'KQLCNT Goi 15', 'QD_15', DATE '2024-06-15', 46000000.0, 50000000.0, 'VND', DATE '2024-06-15', 'IS_PUBLISH', '{}'),
        -- Scenario 16: Unknown status with positive money
        ('rev_res_16', 'res16', 'result', 'obs_p16', TRUE, 'RES_016', '00', NULL, NULL, 'p16', 'PKG_016', 'KQLCNT Goi 16 Chua Ro', 'QD_16', DATE '2024-06-20', 48000000.0, 50000000.0, 'VND', DATE '2024-06-20', 'STATUS_REVIEW', '{}'),
        -- Scenario 17: Baseline after award (award 2024-06-01, baseline dated 2024-08-01)
        ('rev_res_17', 'res17', 'result', 'obs_p17', TRUE, 'RES_017', '00', NULL, NULL, 'p17', 'PKG_017', 'KQLCNT Goi 17', 'QD_17', DATE '2024-06-01', 45000000.0, 50000000.0, 'VND', DATE '2024-06-01', 'IS_PUBLISH', '{}'),
        -- Scenario 18: USD Result
        ('rev_res_18', 'res18', 'result', 'obs_p18', TRUE, 'RES_018', '00', NULL, NULL, 'p18', 'PKG_018', 'KQLCNT Goi 18 USD', 'QD_18', DATE '2024-07-01', 180000.0, 200000.0, 'USD', DATE '2024-07-01', 'IS_PUBLISH', '{}');
    """)

    # Award Lots
    con.execute("""
        INSERT INTO silver.award_lot (
            award_lot_id, revision_id, lot_no, lot_name, lot_price, winning_price, currency, status
        ) VALUES
        -- P1 Lot 1 (2024): Baseline 60M, Winning 54M
        ('alot_1_1', 'rev_res_1_2024', '1', 'Lo 1', 60000000.0, 54000000.0, 'VND', 'HOAT_DONG'),
        -- P1 Lot 2 (2025): Baseline 40M, Winning 38M
        ('alot_1_2', 'rev_res_1_2025', '2', 'Lo 2', 40000000.0, 38000000.0, 'VND', 'HOAT_DONG'),
        -- P2 Lot 1: Baseline 120M, Winning 100M
        ('alot_2_1', 'rev_res_2', '1', 'Lo 1', 120000000.0, 100000000.0, 'VND', 'HOAT_DONG'),
        -- P3 Lot 1: Baseline 100M, Winning 90M
        ('alot_3_1', 'rev_res_3', '1', 'Lo 1', 100000000.0, 90000000.0, 'VND', 'HOAT_DONG'),
        -- P5 Lot 1: Baseline 50M, Winning 45M
        ('alot_5_1', 'rev_res_5', '1', 'Lo 1', 50000000.0, 45000000.0, 'VND', 'HOAT_DONG'),
        -- P7 Cancelled Lot
        ('alot_7_1', 'rev_res_7', '1', 'Lo 1', 40000000.0, 35000000.0, 'VND', 'HUY'),
        -- P8 Baseline 2020
        ('alot_8_1', 'rev_res_8', '1', 'Lo 1', 90000000.0, 80000000.0, 'VND', 'HOAT_DONG'),
        -- P9 2026 result
        ('alot_9_1', 'rev_res_9', '1', 'Lo 1', 70000000.0, 65000000.0, 'VND', 'HOAT_DONG'),
        -- P10 Unknown currency
        ('alot_10_1', 'rev_res_10', '1', 'Lo 1', 60000000.0, 55000000.0, 'UNKNOWN', 'HOAT_DONG'),
        -- P11 Negative savings (110M vs 100M)
        ('alot_11_1', 'rev_res_11', '1', 'Lo 1', 100000000.0, 110000000.0, 'VND', 'HOAT_DONG'),
        -- P13A Lot 1
        ('alot_13a', 'rev_res_13a', '1', 'Lo 1 Goi 13A', 40000000.0, 36000000.0, 'VND', 'HOAT_DONG'),
        -- P13B Lot 1
        ('alot_13b', 'rev_res_13b', '1', 'Lo 1 Goi 13B', 60000000.0, 54000000.0, 'VND', 'HOAT_DONG'),
        -- P14: Two lots both named '1' with distinct source IDs
        ('alot_14_1', 'rev_res_14', '1', 'Phan A Lo 1', 35000000.0, 32000000.0, 'VND', 'HOAT_DONG'),
        ('alot_14_2', 'rev_res_14', '1', 'Phan B Lo 1', 35000000.0, 33000000.0, 'VND', 'HOAT_DONG'),
        -- P15 Lot 1
        ('alot_15_1', 'rev_res_15', '1', 'Lo 1 Goi 15', 50000000.0, 46000000.0, 'VND', 'HOAT_DONG'),
        -- P16 Lot 1 (unknown status result)
        ('alot_16_1', 'rev_res_16', '1', 'Lo 1 Goi 16', 50000000.0, 48000000.0, 'VND', 'HOAT_DONG'),
        -- P17 Lot 1 (baseline after award)
        ('alot_17_1', 'rev_res_17', '1', 'Lo 1 Goi 17', 50000000.0, 45000000.0, 'VND', 'HOAT_DONG'),
        -- P18 Lot 1 (USD)
        ('alot_18_1', 'rev_res_18', '1', 'Lo 1 Goi 18 USD', 200000.0, 180000.0, 'USD', 'HOAT_DONG');
    """)

    # Award Contractors
    con.execute("""
        INSERT INTO silver.award_contractor (
            award_contractor_id, award_lot_id, revision_id,
            contractor_code, contractor_name, tax_code, country,
            is_consortium, venture_code, venture_name, role,
            awarded_price, bid_result, currency
        ) VALUES
        -- P1 Lot 1 Winner
        ('actr_1_1', 'alot_1_1', 'rev_res_1_2024', 'CTR_A', 'Cong ty Xay dung A', '01001', 'VN', FALSE, NULL, NULL, 'Chinh', 54000000.0, 1, 'VND'),
        -- P1 Lot 2 Winner
        ('actr_1_2', 'alot_1_2', 'rev_res_1_2025', 'CTR_B', 'Cong ty Xay dung B', '01002', 'VN', FALSE, NULL, NULL, 'Chinh', 38000000.0, 1, 'VND'),
        -- P2 Lot 1: Two independent winners each 50M (same price)
        ('actr_2_1', 'alot_2_1', 'rev_res_2', 'CTR_C1', 'Cong ty Thiet bi C1', '01003', 'VN', FALSE, NULL, NULL, 'Doc lap', 50000000.0, 1, 'VND'),
        ('actr_2_2', 'alot_2_1', 'rev_res_2', 'CTR_C2', 'Cong ty Thiet bi C2', '01004', 'VN', FALSE, NULL, NULL, 'Doc lap', 50000000.0, 1, 'VND'),
        -- P3 Lot 1: Consortium CON_01 with members M1 and M2
        ('actr_3_1', 'alot_3_1', 'rev_res_3', 'CTR_M1', 'Thanh vien Lien danh M1', '01005', 'VN', TRUE, 'CON_01', 'Lien danh M1-M2', 'Dung dau lien danh', 90000000.0, 1, 'VND'),
        ('actr_3_2', 'alot_3_1', 'rev_res_3', 'CTR_M2', 'Thanh vien Lien danh M2', '01006', 'VN', TRUE, 'CON_01', 'Lien danh M1-M2', 'Thanh vien', 90000000.0, 1, 'VND'),
        -- P5 Lot 1 Winner (direct package, missing notice)
        ('actr_5_1', 'alot_5_1', 'rev_res_5', 'CTR_E', 'Cong ty Tu van E', '01007', 'VN', FALSE, NULL, NULL, 'Chinh', 45000000.0, 1, 'VND'),
        -- P7 Cancelled award winner
        ('actr_7_1', 'alot_7_1', 'rev_res_7', 'CTR_G', 'Cong ty G', '01008', 'VN', FALSE, NULL, NULL, 'Chinh', 35000000.0, 1, 'VND'),
        -- P8 Winner
        ('actr_8_1', 'alot_8_1', 'rev_res_8', 'CTR_H', 'Cong ty H', '01009', 'VN', FALSE, NULL, NULL, 'Chinh', 80000000.0, 1, 'VND'),
        -- P9 Winner
        ('actr_9_1', 'alot_9_1', 'rev_res_9', 'CTR_I', 'Cong ty I', '01010', 'VN', FALSE, NULL, NULL, 'Chinh', 65000000.0, 1, 'VND'),
        -- P10 Winner
        ('actr_10_1', 'alot_10_1', 'rev_res_10', 'CTR_K', 'Cong ty K', '01011', 'VN', FALSE, NULL, NULL, 'Chinh', 55000000.0, 1, 'UNKNOWN'),
        -- P11 Winner (negative savings)
        ('actr_11_1', 'alot_11_1', 'rev_res_11', 'CTR_L', 'Cong ty L', '01012', 'VN', FALSE, NULL, NULL, 'Chinh', 110000000.0, 1, 'VND'),
        -- P13A Winner
        ('actr_13a', 'alot_13a', 'rev_res_13a', 'CTR_13A', 'Cong ty 13A', '01013', 'VN', FALSE, NULL, NULL, 'Chinh', 36000000.0, 1, 'VND'),
        -- P13B Winner
        ('actr_13b', 'alot_13b', 'rev_res_13b', 'CTR_13B', 'Cong ty 13B', '01014', 'VN', FALSE, NULL, NULL, 'Chinh', 54000000.0, 1, 'VND'),
        -- P14 Winners
        ('actr_14_1', 'alot_14_1', 'rev_res_14', 'CTR_14A', 'Cong ty 14A', '01015', 'VN', FALSE, NULL, NULL, 'Chinh', 32000000.0, 1, 'VND'),
        ('actr_14_2', 'alot_14_2', 'rev_res_14', 'CTR_14B', 'Cong ty 14B', '01016', 'VN', FALSE, NULL, NULL, 'Chinh', 33000000.0, 1, 'VND'),
        -- P15 Winner
        ('actr_15_1', 'alot_15_1', 'rev_res_15', 'CTR_15', 'Cong ty 15', '01017', 'VN', FALSE, NULL, NULL, 'Chinh', 46000000.0, 1, 'VND'),
        -- P16 Winner (unconfirmed status)
        ('actr_16_1', 'alot_16_1', 'rev_res_16', 'CTR_16', 'Cong ty 16', '01018', 'VN', FALSE, NULL, NULL, 'Chinh', 48000000.0, 1, 'VND'),
        -- P17 Winner
        ('actr_17_1', 'alot_17_1', 'rev_res_17', 'CTR_17', 'Cong ty 17', '01019', 'VN', FALSE, NULL, NULL, 'Chinh', 45000000.0, 1, 'VND'),
        -- P18 Winner (USD)
        ('actr_18_1', 'alot_18_1', 'rev_res_18', 'CTR_18', 'Cong ty 18 USD', '01020', 'US', FALSE, NULL, NULL, 'Chinh', 180000.0, 1, 'USD');
    """)
