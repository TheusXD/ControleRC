/**
 * Este código utiliza a sintaxe V2 do Firebase Functions, que é a mais moderna.
 */
const {onDocumentCreated} = require("firebase-functions/v2/firestore");
const {initializeApp} = require("firebase-admin/app");
const {getFirestore} = require("firebase-admin/firestore");
const nodemailer = require("nodemailer");
const logger = require("firebase-functions/logger");

// Carrega as variáveis de ambiente do arquivo .env.production
require("dotenv").config({ path: '.env.production' });

// Inicializa o app do Firebase Admin
initializeApp();

// --- Configuração do Transportador de E-mail ---
const gmailEmail = process.env.GMAIL_EMAIL;
const gmailPassword = process.env.GMAIL_PASSWORD;

const mailTransport = nodemailer.createTransport({
  service: "gmail",
  auth: {
    user: gmailEmail,
    pass: gmailPassword,
  },
});

/**
 * Cloud Function que é acionada sempre que um novo documento é criado
 * na coleção 'pedidos'.
 */
exports.enviarNotificacaoNovoPedido = onDocumentCreated("pedidos/{pedidoId}", async (event) => {
    const snap = event.data;
    if (!snap) {
        logger.log("Nenhum dado associado ao evento.");
        return;
    }
    const novoPedido = snap.data();

    // --- LÓGICA ATUALIZADA ---
    // 1. Verifica se um e-mail de notificação foi fornecido no pedido.
    const emailDestino = novoPedido.email_notificacao;

    if (!emailDestino) {
        logger.log(`Nenhum e-mail de notificação fornecido para o pedido ${novoPedido.numero_pedido}. A função será encerrada.`);
        return; // Encerra a função se não houver e-mail.
    }

    const numeroPedido = novoPedido.numero_pedido || "Sem Número";
    const valorPedido = novoPedido.valor.toFixed(2).replace(".", ",");

    try {
        // 2. Busca a Requisição (RC) para obter a descrição da demanda original.
        const requisicaoDoc = await getFirestore()
            .collection("requisicoes")
            .doc(novoPedido.requisicao_id)
            .get();
        if (!requisicaoDoc.exists) {
            logger.log(`Requisição com ID ${novoPedido.requisicao_id} não encontrada.`);
            return;
        }
        const requisicaoData = requisicaoDoc.data();
        const demandaId = requisicaoData.demanda_id;

        // 3. Busca a Demanda original para obter a descrição.
        const demandaDoc = await getFirestore()
            .collection("demandas")
            .doc(demandaId)
            .get();
        if (!demandaDoc.exists) {
            logger.log(`Demanda com ID ${demandaId} não encontrada.`);
            return;
        }
        const demandaData = demandaDoc.data();
        const descricaoDemanda = demandaData.descricao_necessidade;
        const solicitanteUsername = demandaData.solicitante_demanda;


        // 4. Montar e enviar o e-mail de notificação para o endereço fornecido.
        const mailOptions = {
            from: `"Sistema de Compras" <${gmailEmail}>`,
            to: emailDestino,
            subject: `✅ Novo Pedido Gerado: ${numeroPedido}`,
            html: `
            <p>Olá!</p>
            <p>Um novo pedido de compra foi gerado no sistema.</p>
            <hr>
            <h3>Detalhes do Pedido</h3>
            <ul>
              <li><strong>Número do Pedido:</strong> ${numeroPedido}</li>
              <li><strong>Valor:</strong> R$ ${valorPedido}</li>
              <li><strong>Solicitante da Demanda Original:</strong> ${solicitanteUsername}</li>
            </ul>
            <h3>Demanda Original</h3>
            <p><strong>Descrição:</strong> ${descricaoDemanda}</p>
            <hr>
            <p>Você pode acompanhar o status do pedido através do sistema.</p>
            <p><em>Esta é uma mensagem automática, por favor, não responda.</em></p>
          `,
        };

        await mailTransport.sendMail(mailOptions);
        logger.log(`Notificação enviada com sucesso para ${emailDestino}`);

    } catch (error) {
        logger.error("Erro ao enviar notificação por e-mail:", error);
    }
});
