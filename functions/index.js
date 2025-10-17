/**
 * Versão final usando Nodemailer com Gmail diretamente.
 * Esta é a solução mais confiável para entrega de e-mail.
 */
const {onDocumentCreated} = require("firebase-functions/v2/firestore");
const {initializeApp} = require("firebase-admin/app");
const {getFirestore} = require("firebase-admin/firestore");
const nodemailer = require("nodemailer");
const logger = require("firebase-functions/logger");

initializeApp();

exports.enviarNotificacaoNovoPedido = onDocumentCreated(
  {
    document: "pedidos/{pedidoId}",
    // Declara os secrets que a função usará
    secrets: ["GMAIL_EMAIL", "GMAIL_PASSWORD"],
  },
  async (event) => {
    const snap = event.data;
    if (!snap) { return; }
    const novoPedido = snap.data();

    const gmailEmail = process.env.GMAIL_EMAIL;
    const gmailPassword = process.env.GMAIL_PASSWORD;

    if (!gmailEmail || !gmailPassword) {
        logger.error("GMAIL_EMAIL ou GMAIL_PASSWORD não encontrados nos secrets.");
        return;
    }

    // Configura o transporte para o Gmail
    const mailTransport = nodemailer.createTransport({
      service: "gmail",
      auth: {
        user: gmailEmail,
        pass: gmailPassword,
      },
    });

    const emailsString = novoPedido.email_notificacao;
    if (!emailsString || emailsString.trim() === "") {
        logger.log("Nenhum e-mail de notificação para o pedido.");
        return;
    }

    const emailList = emailsString.split(',').map(email => email.trim()).filter(email => email);
    if (emailList.length === 0) {
        logger.log("A lista de e-mails está vazia.");
        return;
    }

    // O resto do código continua igual...
    const numeroPedido = novoPedido.numero_pedido || "Sem Número";
    const valorPedido = novoPedido.valor.toFixed(2).replace(".", ",");

    try {
        const requisicaoDoc = await getFirestore().collection("requisicoes").doc(novoPedido.requisicao_id).get();
        const requisicaoData = requisicaoDoc.exists ? requisicaoDoc.data() : {};
        const demandaDoc = requisicaoData.demanda_id ? await getFirestore().collection("demandas").doc(requisicaoData.demanda_id).get() : null;
        const demandaData = demandaDoc && demandaDoc.exists ? demandaDoc.data() : {};
        const descricaoDemanda = demandaData.descricao_necessidade || "N/A";
        const solicitanteUsername = demandaData.solicitante_demanda || "N/A";

        const mailOptions = {
            from: `"Sistema de Compras" <${gmailEmail}>`,
            to: emailList.join(", "),
            subject: `✅ Novo Pedido Gerado: ${numeroPedido}`,
            html: `(O seu corpo do e-mail em HTML aqui)`,
            attachments: []
        };

        if (novoPedido.anexo_email && novoPedido.anexo_email.b64_data) {
            mailOptions.attachments.push({
                filename: novoPedido.anexo_email.file_name,
                content: Buffer.from(novoPedido.anexo_email.b64_data, 'base64'),
                contentType: novoPedido.anexo_email.content_type,
            });
        }

        await mailTransport.sendMail(mailOptions);
        logger.log(`E-mail enviado via Gmail para: ${emailList.join(", ")}`);

    } catch (error) {
        logger.error("Erro ao enviar notificação por e-mail:", error);
    }
});