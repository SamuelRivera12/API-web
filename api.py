from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pyodbc
from fastapi.middleware.cors import CORSMiddleware
import datetime
from typing import List

app = FastAPI()

SERVER = '16.171.148.89,1433'  # IP + Puerto
DATABASE = 'Exides'
USERNAME = 'samu'
PASSWORD = 'ContraSql'

# Configurar CORS para permitir solicitudes desde tu frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Reemplaza con el origen de tu frontend en producción
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_connection():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        "Encrypt=no;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

class Producto(BaseModel):
    id_producto: int | None = None
    nombre: str
    descripcion: str
    categoria: str
    marca: str
    tipo: str
    precio: float
    unidades: int
    foto: str | None = None

# Modelo para datos de ventas mensuales
class SalesData(BaseModel):
    month: str
    amount: float

# Modelo para datos de ventas por categoría
class CategorySalesData(BaseModel):
    category: str
    amount: float

class LowStockCount(BaseModel):
    stock: int

class Users(BaseModel):
    id: int | None = None
    name: str
    email: str
    role: str
    ultima_sesion: str | None = None
    estado: str | None = None

class UserUpdate(BaseModel):
    name: str
    email: str
    role: str
    estado: str
    password: str | None = None

class ProductUpdate(BaseModel):
    nombre: str
    descripcion: str
    categoria: str
    tipo: str
    precio: float
    unidades: int
    foto: str | None = None



class LineaPedido(BaseModel):
    id_producto: int
    cantidad: int
    precio: float

class Pedido(BaseModel):
    id_usuario: int
    fecha_pedido: str
    estado: str
    direccion: str
    ciudad: str
    pais: str
    codigo_postal: str
    metodo_pago: str
    cantidad_total: float
    lineas: List[LineaPedido]

@app.post("/pedidos")
async def crear_pedido(pedido: Pedido):
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # First insert into Pedidos
        query_pedido = """
            INSERT INTO Pedidos (id_usuario, fecha_pedido, estado, direccion, 
                               ciudad, pais, codigo_postal, metodo_pago, cantidad_total)
            OUTPUT INSERTED.id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(query_pedido, (
            pedido.id_usuario,
            datetime.datetime.now(),
            pedido.estado,
            pedido.direccion,
            pedido.ciudad,
            pedido.pais,
            pedido.codigo_postal,
            pedido.metodo_pago,
            pedido.cantidad_total
        ))
        pedido_id = cursor.fetchone()[0]

        # Then insert sales records for each product
        for linea in pedido.lineas:
            # Calculate total for this line
            total_linea = linea.cantidad * linea.precio
            
            query_venta = """
                INSERT INTO Ventas (producto_id, fecha_venta, cantidad, total)
                OUTPUT INSERTED.id
                VALUES (?, ?, ?, ?)
            """
            cursor.execute(query_venta, (
                linea.id_producto,
                datetime.datetime.now(),
                linea.cantidad,
                total_linea
            ))
            venta_id = cursor.fetchone()[0]

            # Insert order lines using the sale ID
            query_linea = """
                INSERT INTO Linea_pedidos (id_orden, id_producto, cantidad, precio)
                VALUES (?, ?, ?, ?)
            """
            cursor.execute(query_linea, (
                venta_id,
                linea.id_producto,
                linea.cantidad,
                linea.precio
            ))

            # Update product stock
            query_stock = """
                UPDATE Productos 
                SET unidades = unidades - ?
                WHERE id_producto = ?
            """
            cursor.execute(query_stock, (linea.cantidad, linea.id_producto))

        conn.commit()
        return {
            "message": "Pedido creado correctamente",
            "id_pedido": pedido_id
        }

    except Exception as e:
        if 'conn' in locals():
            conn.rollback()
        print(f"Error creating order: {str(e)}")  # Debug print
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals():
            conn.close()
            
@app.get("/pedidos/recientes")
def get_recent_orders():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT TOP 5
                p.*, 
                u.name as cliente_nombre
            FROM Pedidos p
            JOIN users u ON p.id_usuario = u.id
            ORDER BY p.fecha_pedido DESC
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()
        
        return [
            {
                "id": row.id,
                "cliente_nombre": row.cliente_nombre,
                "fecha": row.fecha_pedido.strftime('%Y-%m-%d'),
                "estado": row.estado,
                "cantidad_total": float(row.cantidad_total),
                "direccion": row.direccion,
                "ciudad": row.ciudad,
                "pais": row.pais,
                "codigo_postal": row.codigo_postal,
                "metodo_pago": row.metodo_pago
            }
            for row in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pedidos/{pedido_id}")
def get_pedido(pedido_id: int):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Obtener datos del pedido
        query = """
            SELECT p.*, u.name as nombre_cliente, u.email
            FROM Pedidos p
            JOIN users u ON p.id_usuario = u.id
            WHERE p.id = ?
        """
        cursor.execute(query, (pedido_id,))
        pedido = cursor.fetchone()
        
        if not pedido:
            raise HTTPException(status_code=404, detail="Pedido no encontrado")
        
        # Obtener líneas del pedido
        query = """
            SELECT lp.*, pr.nombre as nombre_producto
            FROM Linea_pedidos lp
            JOIN Productos pr ON lp.id_producto = pr.id_producto
            WHERE lp.id_orden = ?
        """
        cursor.execute(query, (pedido_id,))
        lineas = cursor.fetchall()
        
        conn.close()
        
        return {
            "id": pedido.id,
            "cliente_nombre": pedido.nombre_cliente,
            "cliente_email": pedido.email,
            "fecha": pedido.fecha_pedido.strftime('%Y-%m-%d'),
            "estado": pedido.estado,
            "direccion": pedido.direccion,
            "ciudad": pedido.ciudad,
            "pais": pedido.pais,
            "codigo_postal": pedido.codigo_postal,
            "metodo_pago": pedido.metodo_pago,
            "total": float(pedido.cantidad_total),
            "productos": [
                {
                    "id": linea.id_producto,
                    "nombre": linea.nombre_producto,
                    "cantidad": linea.cantidad,
                    "precio": float(linea.precio),
                    "subtotal": float(linea.precio * linea.cantidad)
                }
                for linea in lineas
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/pedidos")
def get_pedidos():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Obtener todos los pedidos
        query = """
            SELECT p.*, u.name as nombre_cliente, u.email
            FROM Pedidos p
            JOIN users u ON p.id_usuario = u.id
        """
        cursor.execute(query)
        pedidos = cursor.fetchall()
        
        conn.close()
        
        return [
            {
                "id": pedido.id,
                "cliente_nombre": pedido.nombre_cliente,
                "cliente_email": pedido.email,
                "fecha": pedido.fecha_pedido.strftime('%Y-%m-%d'),
                "total": float(pedido.cantidad_total),
                "estado": pedido.estado,
                "direccion": pedido.direccion,
                "ciudad": pedido.ciudad,
                "pais": pedido.pais,
                "codigo_postal": pedido.codigo_postal,
                "metodo_pago": pedido.metodo_pago
                
            }
            for pedido in pedidos
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/pedidos/{pedido_id}")
def update_pedido(pedido_id: int, pedido: Pedido):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Actualizar pedido
        query = """
            UPDATE Pedidos
            SET estado = ?, direccion = ?, ciudad = ?, 
                pais = ?, codigo_postal = ?, metodo_pago = ?
            WHERE id = ?
        """
        cursor.execute(query, (
            pedido.estado,
            pedido.direccion,
            pedido.ciudad,
            pedido.pais,
            pedido.codigo_postal,
            pedido.metodo_pago,
            pedido_id
        ))
        
        # Actualizar líneas de pedido
        if pedido.lineas:
            # Eliminar líneas existentes
            cursor.execute("DELETE FROM Linea_pedidos WHERE id_orden = ?", (pedido_id,))
            
            # Insertar nuevas líneas
            for linea in pedido.lineas:
                query = """
                    INSERT INTO Linea_pedidos (id_orden, id_producto, cantidad, precio)
                    VALUES (?, ?, ?, ?)
                """
                cursor.execute(query, (
                    pedido_id,
                    linea.id_producto,
                    linea.cantidad,
                    linea.precio
                ))
        
        conn.commit()
        conn.close()
        
        return {"message": "Pedido actualizado correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/productos/{id_producto}")
def editar_producto(id_producto: int, producto: ProductUpdate):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        query = """
            UPDATE Productos
            SET nombre = ?, descripcion = ?, categoria = ?, tipo = ?, 
                precio = ?, unidades = ?, foto = ?
            WHERE id_producto = ?
        """
        
        cursor.execute(query, (
            producto.nombre,
            producto.descripcion,
            producto.categoria,
            producto.tipo,
            producto.precio,
            producto.unidades,
            producto.foto,
            id_producto
        ))
        
        conn.commit()
        conn.close()
        
        return {"message": "Producto actualizado correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al actualizar producto: {str(e)}")
# Endpoint para datos de productos
@app.get("/productos", response_model=List[Producto])
def get_Productos():
    conn = get_connection()
    cursor = conn.cursor()
    query = "SELECT * FROM Productos"
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    
    return [
        {
            "id_producto": row.id_producto,
            "nombre": row.nombre,
            "descripcion": row.descripcion,
            "categoria": row.categoria,
            "marca": row.marca,
            "tipo": row.tipo,
            "precio": float(row.precio),
            "unidades": row.unidades,
            "foto": row.foto

        }
        for row in rows
    ]

@app.get("/productos/mas_vendidos")
def get_top_products():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT TOP 5
                p.nombre,
                p.categoria,
                p.precio,
                COUNT(lp.id_producto) as unidades_vendidas,
                SUM(lp.cantidad * lp.precio) as ingresos_totales
            FROM Productos p
            LEFT JOIN Linea_pedidos lp ON p.id_producto = lp.id
            GROUP BY p.nombre, p.categoria, p.precio
            ORDER BY unidades_vendidas DESC
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()
        
        return [
            {
                "nombre": row.nombre,
                "categoria": row.categoria,
                "precio": float(row.precio),
                "unidades_vendidas": row.unidades_vendidas or 0,
                "ingresos_totales": float(row.ingresos_totales or 0)
            }
            for row in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/ventas/mensuales_ultimo_mes", response_model=List[SalesData])
def get_monthly_sales():
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            # Asumiendo que tienes una tabla 'sales' con columnas 'amount' y 'date'
            query = """
            SELECT 
                MONTH(fecha_venta) AS month_num,
                FORMAT(fecha_venta, 'MMM') AS month_name,
                SUM(total) AS total_amount
            FROM 
                Ventas
            
            WHERE 
                fecha_venta >= DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1)
                AND fecha_venta < DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)
            GROUP BY 
                MONTH(fecha_venta),
                FORMAT(fecha_venta, 'MMM')

            """
            
            cursor.execute(query)
            rows = cursor.fetchall()
            conn.close()
            
            return [
                {"month": row.month_name, "amount": float(row.total_amount)}
                for row in rows
            ]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error al obtener datos de ventas mensuales: {str(e)}")
        
@app.get("/ventas/mensual", response_model=List[SalesData])
def get_monthly_sales():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Consulta adaptada a la tabla Ventas con nombres en español
        query = """
        SELECT 
            MONTH(fecha_venta) as month_num,
            FORMAT(fecha_venta, 'MMM') as month_name,
            SUM(total) as total_amount
        FROM 
            Ventas
        WHERE 
            fecha_venta >= DATEADD(YEAR, -1, GETDATE())
        GROUP BY 
            MONTH(fecha_venta),
            FORMAT(fecha_venta, 'MMM')
        ORDER BY 
            MONTH(fecha_venta)
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()
        
        # Aquí adaptamos los nombres de campo para que coincidan con SalesData
        return [
            {"month": row.month_name, "amount": float(row.total_amount)}
            for row in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener datos de ventas mensuales: {str(e)}")
    

        
@app.get("/ventas/total_ultimo_mes", response_model=List[SalesData])
def get_last_month_order_count():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Consulta para obtener el conteo de pedidos del mes anterior
        query = """
            SELECT 
                MONTH(fecha_venta) AS month_num,
                FORMAT(fecha_venta, 'MMM') AS month_name,
                COUNT(*) AS total_pedidos
            FROM 
                Ventas
            WHERE 
                fecha_venta >= DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1)
                AND fecha_venta < DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)
            GROUP BY
                MONTH(fecha_venta),
                FORMAT(fecha_venta, 'MMM')
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()
        
        return [
            {"month": row.month_name, "amount": float(row.total_pedidos)}
            for row in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener conteo de pedidos del último mes: {str(e)}")
    
@app.get("/productos/poco_stock", response_model=List[LowStockCount])
def get_low_stock_Productos():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        query = """
            SELECT
                COUNT(*) AS cantidad
            FROM
                Productos
            WHERE
                unidades <= 10
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()
        return [{"stock": row.cantidad} for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener productos con bajo stock: {str(e)}")
          


@app.post("/productos")
def create_product(product: ProductUpdate):
    try:
        conn = get_connection()
        cursor = conn.cursor()

        query = """
            INSERT INTO Productos (nombre, tipo, precio, unidades, categoria, descripcion)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        
        cursor.execute(query, (
            product.nombre,
            product.tipo,
            product.precio,
            product.unidades,
            product.categoria,
            product.descripcion
        ))
        
        conn.commit()
        conn.close()

        return {"message": "Producto creado correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al crear producto: {str(e)}")

@app.get("/productos/{product_id}")
def get_product(product_id: int):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT id_producto, nombre, precio, unidades, categoria, descripcion, foto, marca, tipo from Productos
            WHERE id_producto = ?
        """
        cursor.execute(query, (product_id,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        return {
            "id_producto": row.id_producto,
            "nombre": row.nombre,
            "precio": row.precio,
            "unidades": row.unidades,
            "categoria": row.categoria,
            "descripcion": row.descripcion,
            "foto": row.foto,
            "marca": row.marca,
            "tipo": row.tipo
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener producto: {str(e)}")
    
@app.get("/users/users_ultimo_mes")
def get_users_last_month():
    try:
        conn = get_connection()
        cursor = conn.cursor()

        query = """
            SELECT COUNT(*) AS total_users
            FROM users
            WHERE created_at >= DATEADD(MONTH, -1, GETDATE())
            AND created_at < GETDATE()
        """

        cursor.execute(query)
        row = cursor.fetchone()
        conn.close()

        return [{"total_users": row.total_users if row else 0}]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener users: {str(e)}")


@app.get("/users", response_model=List[Users])
def get_users():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        query = "SELECT id, name, email, role, ultima_sesion, estado FROM users"
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()
        
        result = []
        for row in rows:
            last_login_str = None
            if row.ultima_sesion:
                if isinstance(row.ultima_sesion, datetime.datetime):
                    last_login_str = row.ultima_sesion.strftime('%Y-%m-%d')  # Solo día
                else:
                    last_login_str = str(row.ultima_sesion)
            
            result.append({
                "id": row.id,
                "name": row.name,
                "email": row.email,
                "role": row.role,
                "ultima_sesion": last_login_str,
                "estado": row.estado,
            })
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching users: {str(e)}")
    
@app.get("/users/{user_id}", response_model=Users)
def get_user(user_id: int):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        query = "SELECT id, name, email, role, ultima_sesion, estado FROM users WHERE id = ?"
        cursor.execute(query, user_id)
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        
        last_login_str = None
        if row.ultima_sesion:
            if isinstance(row.ultima_sesion, datetime.datetime):
                last_login_str = row.ultima_sesion.strftime('%Y-%m-%d')
            else:
                last_login_str = str(row.ultima_sesion)
        
        return {
                "id": row.id,
                "name": row.name,
                "email": row.email,
                "role": row.role,
                "ultima_sesion": last_login_str,
                "estado": row.estado
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener usuario: {str(e)}")

@app.delete("/productos/{id_producto}")
async def eliminar_producto(id_producto: int):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        query = "DELETE FROM Productos WHERE id_producto = ?"
        cursor.execute(query, (id_producto,))
        
        conn.commit()
        conn.close()
        
        return {"message": "Producto eliminado correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))   
@app.delete("/users/{user_id}")
def delete_user(user_id: int):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        query = "DELETE FROM users WHERE id = ?"
        cursor.execute(query, user_id)
        conn.commit()
        conn.close()
        return {"message": "Usuario eliminado correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al eliminar usuario: {str(e)}")
    


@app.put("/users/{user_id}", response_model=Users)
def update_user(user_id: int, user: UserUpdate):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        if user.password:
            query = """
                UPDATE users
                SET name = ?, email = ?, role = ?, estado = ?, password = ?
                WHERE id = ?
            """
            cursor.execute(query, (user.name, user.email, user.role, 
                                 user.estado, user.password, user_id))
        else:
            query = """
                UPDATE users 
                SET name = ?, email = ?, role = ?, estado = ?
                WHERE id = ?
            """
            cursor.execute(query, (user.name, user.email, user.role, 
                                 user.estado, user_id))
        
        conn.commit()
        conn.close()
        
        return {
            "id": user_id,
            "name": user.name,
            "email": user.email,
            "role": user.role,
            "estado": user.estado,
            "ultima_sesion": None
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al actualizar usuario: {str(e)}")




# # Endpoint para ventas por categoría (para el gráfico de dona)
@app.get("/ventas/categorias", response_model=List[CategorySalesData])
def get_category_sales():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Asumiendo que tienes una relación entre ventas y productos que te permite agrupar por categoría
        query = """
           SELECT 
            p.categoria,
            SUM(s.total) as total_amount
        FROM 
            Ventas s
        JOIN 
            Productos p ON s.producto_id = p.id_producto
        GROUP BY 
            p.categoria
        ORDER BY 
            total_amount DESC

        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()
        
        return [
            {"category": row.categoria, "amount": float(row.total_amount)}
            for row in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener datos de ventas por categoría: {str(e)}")

# # Endpoint para comparar ventas por año (para el gráfico de tendencia)
@app.get("/ventas/tendencia")
def get_sales_trend():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        current_year = datetime.datetime.now().year
        last_year = current_year - 1
        
        # Consulta para obtener ventas mensuales de los últimos dos años
        query = """
        SELECT 
            YEAR(fecha_venta) as year,
            MONTH(fecha_venta) as month_num,
            FORMAT(fecha_venta, 'MMM') as month_name,
            SUM(total) as total_amount
        FROM 
            Ventas
        WHERE 
            fecha_venta >= DATEADD(YEAR, -2, GETDATE())
        GROUP BY 
            YEAR(fecha_venta),
            MONTH(fecha_venta),
            FORMAT(fecha_venta, 'MMM')
        ORDER BY 
            YEAR(fecha_venta),
            MONTH(fecha_venta)
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()
        
        # Organizar datos por año
        current_year_data = []
        last_year_data = []
        months = []
        
        for row in rows:
            if row.year == current_year:
                current_year_data.append(float(row.total_amount))
                if row.month_name not in months:
                    months.append(row.month_name)
            elif row.year == last_year:
                last_year_data.append(float(row.total_amount))
        
        return {
            "months": months,
            "series": [
                {
                    "name": str(last_year),
                    "data": last_year_data
                },
                {
                    "name": str(current_year),
                    "data": current_year_data
                }
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener datos de tendencia de ventas: {str(e)}")

# # Endpoint para ventas por categoría (para el gráfico de barras)
@app.get("/ventas/categoria/detalle")
def get_category_sales_detail():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT 
            p.categoria,
            SUM(s.total) as total_amount
        FROM 
            Ventas s
        JOIN 
            Productos p ON s.producto_id = p.id_producto
        GROUP BY 
            p.categoria
        ORDER BY 
            total_amount DESC
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()
        
        categories = []
        amounts = []
        
        for row in rows:
            categories.append(row.categoria)
            amounts.append(float(row.total_amount))
        
        return {
            "categories": categories,
            "series": [{
                "name": "Ventas",
                "data": amounts
            }]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener datos detallados de ventas por categoría: {str(e)}")