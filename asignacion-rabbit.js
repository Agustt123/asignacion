const amqp = require('amqplib');
const mysql = require('mysql');
const { asignar, desasignar, iniciarProceso } = require('./controllerrabbitmq');
const { conLocal } = require('./db');
const { log } = require('node:console');
const RABBITMQ_URL = 'amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672';
const QUEUE_NAME = 'asignacion';
async function connectRabbitMQ() {
    try {
        const startConnectionTime = performance.now(); // Tiempo de inicio de conexión
        const connection = await amqp.connect(RABBITMQ_URL);
        const endConnectionTime = performance.now(); // Tiempo de finalización de conexión
        const connectionDuration = endConnectionTime - startConnectionTime; // Duración de la conexión

        const channel = await connection.createChannel();
        await channel.assertQueue(QUEUE_NAME, { durable: true });

        console.log(`[*] Esperando mensajes en la cola "${QUEUE_NAME}"`);
        console.log(`Tiempo de conexión a RabbitMQ: ${connectionDuration.toFixed(2)} ms`);

        channel.consume(QUEUE_NAME, async (msg) => {
            if (msg !== null) {
                try {
                    const dataEntrada = JSON.parse(msg.content.toString());
                    console.log("[x] Mensaje recibido:", dataEntrada);

                    // Procesar cualquier mensaje recibido
                    const resultado = await handleOperador(dataEntrada);

                    // Validar que dataEntrada.canal esté definido
                    if (!dataEntrada.canal) {
                        console.error("[x] Error: El campo 'channel' no está definido en el mensaje.");
                        channel.ack(msg); // Confirmar el mensaje
                        return;
                    }

                    // Obtener la hora actual
                    const ahora = new Date();
                    const horaEnvio = ahora.toLocaleTimeString(); // Formato HH:MM:SS

                    const startSendTime = performance.now(); // Tiempo de inicio de envío
                    await channel.sendToQueue(
                        dataEntrada.canal, // Nombre del canal de respuesta
                        Buffer.from(JSON.stringify(resultado)), // Respuesta
                        { persistent: true }
                    );
                    const endSendTime = performance.now(); // Tiempo de finalización de envío
                    const sendDuration = endSendTime - startSendTime; // Duración del envío

                    console.log(`[x] Respuesta enviada al canal ${dataEntrada.canal} a las ${horaEnvio}: `, resultado);
                    console.log(`Tiempo de envío al canal ${dataEntrada.canal}: ${sendDuration.toFixed(2)} ms`);
                } catch (error) {
                    console.error("[x] Error al procesar el mensaje:", error);
                } finally {
                    channel.ack(msg); // Confirmar el procesamiento del mensaje
                }
            }
        });

    } catch (error) {
        console.error("Error al conectar con RabbitMQ:", error);
    }
}


// Función para manejar la lógica del operador
async function handleOperador(dataEntrada) {
    const { operador, empresa, cadete, quien, dataQR } = dataEntrada;

    if (operador === "actualizarEmpresas") {
        // Lógica para actualizar empresas (según sea necesario)
        return;
    }

    if (operador === "getEmpresas") {
        console.log({ empresas: Aempresas });
        return;
    }

    if (empresa == 12 && quien == 49) {
        console.log({ estado: false, mensaje: "Comunicarse con la logística." });
        return;
    }

    const fechaunix = Date.now();
    const sqlLog = `INSERT INTO logs (didempresa, quien, cadete, data, fechaunix) VALUES (?, ?, ?, ?, ?)`;

    try {
        await conLocal.query(sqlLog, [empresa, quien, cadete, JSON.stringify(dataQR), fechaunix]);
    } catch (err) {
        console.error("Error al insertar en logs:", err);
    }

    try {
        const dataQRParsed = dataQR;
        const Aempresas2 = await iniciarProceso();

        if (!Aempresas2[empresa]) {
            console.log({ estado: false, mensaje: "No está cargado el ID de la empresa" });
            return;
        }

        const AdataDB = Aempresas2[empresa];
        if (!AdataDB.dbname || !AdataDB.dbuser || !AdataDB.dbpass) {
            console.log({ estado: false, mensaje: "Error al conectar a la DB" });
            return;
        }

        const con = mysql.createConnection({
            host: "bhsmysql1.lightdata.com.ar",
            user: AdataDB.dbuser,
            password: AdataDB.dbpass,
            database: AdataDB.dbname
        });

        con.connect(err => {
            if (err) {
                console.log({ estado: false, mensaje: err.message });
                return;
            }
        });

        const isFlex = dataQRParsed.hasOwnProperty("sender_id");
        const didenvio = isFlex ? 0 : dataQRParsed.did;

        if (!isFlex) {
          
          return  handleRegularPackage(didenvio, empresa, cadete, quien, con, dataQRParsed);
          
        } else {
            handleFlexPackage(dataQRParsed.id, con, cadete, empresa);
        }
    } catch (error) {
      
        
        console.error("Error en el manejo del operador:", error);
    }
}

// Función para manejar paquetes regulares
async function handleRegularPackage(didenvio, empresa, cadete, quien, con, dataQRParsed) {
    const didempresapaquete = dataQRParsed.empresa;
    
    if (empresa != didempresapaquete) {
        const sql = `SELECT didLocal FROM envios_exteriores WHERE superado=0 AND elim=0 AND didExterno = ? AND didEmpresa = ?`;
        try {
           
            const rows = await query(con, sql, [didenvio, didempresapaquete]);

            if (rows.length > 0) {
                const didLocal = rows[0]["didLocal"];
                cadete !== -2 ? asignar(didLocal, empresa, cadete, quien) : desasignar(didLocal, empresa, cadete, quien);

            } else {
                console.log({ estado: false, mensaje: "El paquete externo no existe en la logística." });
            }
        } catch (err) { 
            console.error("Error en consulta de envios_exteriores:", err);
        }
    } else {
  
        
            return cadete !== -2 ? asignar(didenvio, empresa, cadete, quien) : desasignar(didenvio, empresa, cadete, quien);
        
    }
}

// Función para manejar paquetes flexibles
function handleFlexPackage(idshipment, con, cadete, empresa) {
    const query = `SELECT did FROM envios WHERE flex=1 AND superado=0 AND elim=0 AND ml_shipment_id = ?`;
    con.query(query, [idshipment], (err, rows) => {
        if (err) {
            console.log({ estado: false, mensaje: "Error en la consulta de paquete flexible." });
            return;
        }

        const Aresult = Object.values(JSON.parse(JSON.stringify(rows)));
        con.end();

        if (Aresult.length > 0) {
            const didenvio = Aresult[0]["did"];
            cadete !== -2 ? asignar(didenvio, empresa, cadete, quien) : desasignar(didenvio, empresa, cadete, quien);
        } else {
            console.log({ estado: false, mensaje: "El paquete flexible no se encontró en la base de datos." });
        }
    });
}

// Función para manejar consultas SQL con Promesas
function query(connection, sql, params) {
    return new Promise((resolve, reject) => {
        connection.query(sql, params, (error, results) => {
            if (error) return reject(error);
            resolve(results);
        });
    });
}

// Iniciar la conexión a RabbitMQ
connectRabbitMQ();




