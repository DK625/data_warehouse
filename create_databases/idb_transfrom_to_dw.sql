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
