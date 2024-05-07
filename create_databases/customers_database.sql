-- Tạo bảng Thành phố trước (tạo thêm)
CREATE TABLE ThanhPho (
    MaThanhPho SERIAL PRIMARY KEY,
    TenThanhPho VARCHAR(256),
    DiaChi VARCHAR(256),
    Bang VARCHAR(256),
    ThoiGian DATE
);

-- Tạo bảng Khách hàng
CREATE TABLE KhachHang (
    MaKH SERIAL PRIMARY KEY,
    TenKH VARCHAR(256),
    MaThanhPho INTEGER REFERENCES ThanhPho(MaThanhPho),
    NgayDatHangDauTien DATE
);

-- Tạo bảng Khách hàng du lịch
CREATE TABLE KhachHangDuLich (
    MaKH INTEGER PRIMARY KEY REFERENCES KhachHang(MaKH),
    HuongDanVienDuLich VARCHAR(256),
    ThoiGian DATE
);

-- Tạo bảng Khách hàng bưu điện
CREATE TABLE KhachHangBuuDien (
    MaKH INTEGER PRIMARY KEY REFERENCES KhachHang(MaKH),
    DiaChiBuuDien VARCHAR(256),
    ThoiGian DATE
);
