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

    // --- LÓGICA ATUALIZADA PARA MÚLTIPLOS E-MAILS E ANEXOS ---
    const emailsString = novoPedido.email_notificacao;

    if (!emailsString || emailsString.trim() === "") {
        logger.log(`Nenhum e-mail de notificação fornecido para o pedido ${novoPedido.numero_pedido}. A função será encerrada.`);
        return;
    }

    // Separa a string de e-mails em uma lista, removendo espaços em branco
    const emailList = emailsString.split(',').map(email => email.trim()).filter(email => email);

    if (emailList.length === 0) {
        logger.log("A lista de e-mails está vazia após o processamento.");
        return;
    }

    const numeroPedido = novoPedido.numero_pedido || "Sem Número";
    const valorPedido = novoPedido.valor.toFixed(2).replace(".", ",");

    try {
        const requisicaoDoc = await getFirestore().collection("requisicoes").doc(novoPedido.requisicao_id).get();
        if (!requisicaoDoc.exists) {
            logger.log(`Requisição com ID ${novoPedido.requisicao_id} não encontrada.`);
            return;
        }
        const requisicaoData = requisicaoDoc.data();
        const demandaId = requisicaoData.demanda_id;

        const demandaDoc = await getFirestore().collection("demandas").doc(demandaId).get();
        if (!demandaDoc.exists) {
            logger.log(`Demanda com ID ${demandaId} não encontrada.`);
            return;
        }
        const demandaData = demandaDoc.data();
        const descricaoDemanda = demandaData.descricao_necessidade;
        const solicitanteUsername = demandaData.solicitante_demanda;

        // Montar o objeto de opções do e-mail
        const mailOptions = {
            from: `"Sistema de Compras" <${gmailEmail}>`,
            to: emailList.join(", "), // Nodemailer aceita uma string de e-mails separados por vírgula
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
          attachments: [] // Inicializa a lista de anexos
        };

        // Adicionar anexo ao e-mail, se existir
        if (novoPedido.anexo_email && novoPedido.anexo_email.b64_data) {
            mailOptions.attachments.push({
                filename: novoPedido.anexo_email.file_name,
                content: Buffer.from(novoPedido.anexo_email.b64_data, 'base64'),
                contentType: novoPedido.anexo_email.content_type,
            });
            logger.log(`Anexando arquivo ${novoPedido.anexo_email.file_name} ao e-mail.`);
        }

        await mailTransport.sendMail(mailOptions);
        logger.log(`Notificação enviada com sucesso para: ${emailList.join(", ")}`);

    } catch (error) {
        logger.error("Erro ao enviar notificação por e-mail:", error);
    }
});
