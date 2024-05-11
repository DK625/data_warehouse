INSERT INTO dw.City_dim (city_id, city_name, office_addr, state)
SELECT CAST(regexp_replace(mathanhpho, '\D', '', 'g') AS INTEGER), tenthanhpho, diachi, bang
FROM public.thanhpho;

INSERT INTO dw.customer_dim (customer_name, tour, post, city_id)
SELECT KH.tenkh,
       CAST(CASE WHEN KHDL.makh IS NOT NULL THEN 1 ELSE 0 END AS bit) AS tour,
       CAST(CASE WHEN KHBD.makh IS NOT NULL THEN 1 ELSE 0 END AS bit) AS post,
	   CAST(regexp_replace(KH.mathanhpho, '\D', '', 'g') AS INTEGER)
FROM public.khachhang KH
LEFT JOIN public.khachhangdulich KHDL ON KH.makh = KHDL.makh
LEFT JOIN public.khachhangbuudien KHBD ON KH.makh = KHBD.makh

INSERT INTO dw.store_dim (phone_number, city_id)
SELECT CH.sodienthoai,
CAST(regexp_replace(CH.mathanhpho, '\D', '', 'g') AS INTEGER)
FROM public.cuahang CH

INSERT INTO dw.sale_fact (time_id, city_id, customer_id, product_id, units_sold, total_amount_sold)
SELECT
	TD.time_id,
	CAST(regexp_replace(KH.mathanhpho, '\D', '', 'g') AS INTEGER) as city_id,
	CAST(regexp_replace(DDH.makhachhang, '\D', '', 'g') AS INTEGER) as customer_id,
	CAST(regexp_replace(MHDD.mamh, '\D', '', 'g') AS INTEGER) as product_id,
    SUM(MHDD.soluongdat)										as units_sold,
    SUM(MHDD.giadat)									as total_amount_sold
FROM
	public.mathangduocdat MHDD
	JOIN dw.time_dim TD ON TD.month_name = TO_CHAR(MHDD.thoigian, 'YYYY-MM')
    JOIN public.dondathang DDH ON DDH.madon = MHDD.madon
	JOIN public.khachhang KH ON DDH.makhachhang = KH.makh
-- WHERE MHDD.mamh = 'MH1238'
GROUP BY
    product_id,
    customer_id,
    time_id,
	city_id

INSERT INTO dw.inventory_fact (time_id, store_id, product_id, quantity)
SELECT
    TD.time_id as time_id,
	CAST(regexp_replace(MHDLT.macuahang, '\D', '', 'g') AS INTEGER) as store_id,
	CAST(regexp_replace(MHDLT.mamh, '\D', '', 'g') AS INTEGER) as product_id,
    SUM(MHDLT.soluongtrongkho) as quantity
FROM
    public.mathangduocluutru MHDLT
	JOIN dw.time_dim TD ON TD.month_name = TO_CHAR(MHDLT.thoigian, 'YYYY-MM')
-- WHERE MHDLT.macuahang = 'CH0163' and MHDLT.mamh = 'MH0792'
GROUP BY
	time_id,
	store_id,
	product_id
