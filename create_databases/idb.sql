-- Tạo bảng Thành phố là bảng Văn phòng đại diện
CREATE TABLE ThanhPho (
    MaThanhPho CHAR(6) PRIMARY KEY, -- Thay SERIAL thành CHAR(6)
    TenThanhPho VARCHAR(255),
    DiaChi VARCHAR(255),
    Bang VARCHAR(255),
    ThoiGian DATE
);

-- Tạo bảng Khách hàng cho đơn đặt hàng (tạo thêm)
CREATE TABLE KhachHang (
    MaKH CHAR(6) PRIMARY KEY, -- Thay SERIAL thành CHAR(6)
    TenKH VARCHAR(255),
    MaThanhPho CHAR(6) REFERENCES ThanhPho(MaThanhPho),
    NgayDatHangDauTien DATE
);

-- Tạo bảng Khách hàng du lịch
CREATE TABLE KhachHangDuLich (
    MaKH CHAR(6) PRIMARY KEY REFERENCES KhachHang(MaKH),
    HuongDanVienDuLich VARCHAR(256),
    ThoiGian DATE
);

-- Tạo bảng Khách hàng bưu điện
CREATE TABLE KhachHangBuuDien (
    MaKH CHAR(6) PRIMARY KEY REFERENCES KhachHang(MaKH),
    DiaChiBuuDien VARCHAR(256),
    ThoiGian DATE
);

-- Tạo bảng Cửa hàng
CREATE TABLE CuaHang (
    MaCuaHang CHAR(6) PRIMARY KEY, -- Thay SERIAL thành CHAR(6)
    MaThanhPho CHAR(6) REFERENCES ThanhPho(MaThanhPho),
    SoDienThoai VARCHAR(20),
    ThoiGian DATE
);

-- Tạo bảng Mặt hàng
CREATE TABLE MatHang (
    MaMH CHAR(6) PRIMARY KEY, -- Thay SERIAL thành CHAR(6)
    MoTa VARCHAR(255),
    KichCo VARCHAR(50),
    TrongLuong FLOAT,
    Gia NUMERIC,
    ThoiGian DATE
);

-- Tạo bảng Mặt hàng được lưu trữ
CREATE TABLE MatHangDuocLuuTru (
	MaMHDLT SERIAL,
    MaCuaHang CHAR(6) REFERENCES CuaHang(MaCuaHang),
    MaMH CHAR(6) REFERENCES MatHang(MaMH),
    SoLuongTrongKho INTEGER,
    ThoiGian DATE,
    PRIMARY KEY (MaMHDLT, MaCuaHang, MaMH)
);

-- Tạo bảng Đơn đặt hàng
CREATE TABLE DonDatHang (
    MaDon CHAR(6) PRIMARY KEY, -- Thay SERIAL thành CHAR(6)
    NgayDatHang DATE,
    MaKhachHang CHAR(6) REFERENCES KhachHang(MaKH)
);

-- Tạo bảng Mặt hàng được đặt
CREATE TABLE MatHangDuocDat (
    MaDon CHAR(6) REFERENCES DonDatHang(MaDon),
    MaMH CHAR(6) REFERENCES MatHang(MaMH),
    SoLuongDat INTEGER,
    GiaDat NUMERIC,
    ThoiGian DATE,
    PRIMARY KEY (MaDon, MaMH)
);
