-- Tạo bảng Thành phố là bảng Văn phòng đại diện
CREATE TABLE ThanhPho (
    MaThanhPho SERIAL PRIMARY KEY,
    TenThanhPho VARCHAR(255),
    DiaChi VARCHAR(255),
    Bang VARCHAR(255),
    ThoiGian DATE
);
-- Tạo bảng Khách hàng cho đơn đặt hàng (tạo thêm)
CREATE TABLE KhachHang (
    MaKH SERIAL PRIMARY KEY,
    TenKH VARCHAR(255),
    MaThanhPho INTEGER REFERENCES ThanhPho(MaThanhPho),
    NgayDatHangDauTien DATE
);

---- Tạo bảng Văn phòng đại diện là bảng thành
--CREATE TABLE VanPhongDaiDien (
--    MaThanhPho SERIAL PRIMARY KEY,
--    TenThanhPho VARCHAR(255),
--    DiaChiVP VARCHAR(255),
--    Bang VARCHAR(255),
--    ThoiGian DATE
--);

-- Tạo bảng Cửa hàng
CREATE TABLE CuaHang (
    MaCuaHang SERIAL PRIMARY KEY,
    MaThanhPho INTEGER REFERENCES ThanhPho(MaThanhPho),
    SoDienThoai VARCHAR(20),
    ThoiGian DATE
);
-- Tạo bảng Mặt hàng
CREATE TABLE MatHang (
    MaMH SERIAL PRIMARY KEY,
    MoTa VARCHAR(255),
    KichCo VARCHAR(50),
    TrongLuong FLOAT,
    Gia NUMERIC,
    ThoiGian DATE
);

-- Tạo bảng Mặt hàng được lưu trữ
CREATE TABLE MatHangDuocLuuTru (
    MaCuaHang INTEGER REFERENCES CuaHang(MaCuaHang),
    MaMH INTEGER REFERENCES MatHang(MaMH),
    SoLuongTrongKho INTEGER,
    ThoiGian DATE,
    PRIMARY KEY (MaCuaHang, MaMH)
);

-- Tạo bảng Đơn đặt hàng
CREATE TABLE DonDatHang (
    MaDon SERIAL PRIMARY KEY,
    NgayDatHang DATE,
    MaKhachHang INTEGER REFERENCES KhachHang(MaKH)
);

-- Tạo bảng Mặt hàng được đặt
CREATE TABLE MatHangDuocDat (
    MaDon INTEGER REFERENCES DonDatHang(MaDon),
    MaMH INTEGER REFERENCES MatHang(MaMH),
    SoLuongDat INTEGER,
    GiaDat NUMERIC,
    ThoiGian DATE,
    PRIMARY KEY (MaDon, MaMH)
);
