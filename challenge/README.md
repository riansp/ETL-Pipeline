1. Menggunakan Delta Load 3 Hari
   Karena data operational biasanya terus bergerak (new, update, cancel), kita ambil window 3 hari ke belakang untuk memastikan perubahan terbaru tetap masuk tanpa full refresh.

2. Extract Dengan Join Langsung
   Join dilakukan langsung di query extract karena ERD sudah jelas relasinya:
     - interaction → order (schedule_id)
     - order → order_item
     - order_item → slip_item → slip
   Ini membuat extract “clean” dan hanya menarik data yang diperlukan.

3. Transform Sederhana di Python
   Transform yang dilakukan:
     - Payment Status (Sudah Dibayar / Belum Dibayar)
     - Total Revenue per Treatment
   Transform dilakukan di Python agar konsisten, maintainable, dan mudah debug.

4. Load: Delete + Insert
   Untuk data mart, metode terbaik untuk delta window adalah:
     - Delete old records in range → Insert updated records
       • Aman untuk update atau perubahan status (misal pending → paid)
       • Aman untuk record baru dan record hilang
       • Tidak perlu UPSERT, proses jauh lebih sederhana

5. Menggunakan SQLAlchemy + pandas.to_sql
Mempercepat insert batch dan mempermudah load tabel mart.
