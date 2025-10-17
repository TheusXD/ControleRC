/**
 * Versão final com SendGrid - mais simples e robusta.
 */
const {onDocumentCreated} = require("firebase-functions/v2/firestore");
const {initializeApp} = require("firebase-admin/app");
const {getFirestore} = require("firebase-admin/firestore");
const sgMail = require("@sendgrid/mail"); // Usa a biblioteca do SendGrid
const logger = require("firebase-functions/logger");

initializeApp();

exports.enviarNotificacaoNovoPedido = onDocumentCreated(
  {
    document: "pedidos/{pedidoId}",
    // Declara os secrets que a função usará (o e-mail é o seu remetente verificado)
    secrets: ["SENDGRID_API_KEY", "GMAIL_EMAIL"],
  },
  async (event) => {
    const snap = event.data;
    if (!snap) {
        logger.log("Nenhum dado associado ao evento.");
        return;
    }
    const novoPedido = snap.data();

    // Configura o SendGrid com a chave de API
    sgMail.setApiKey(process.env.SENDGRID_API_KEY);
    const fromEmail = process.env.GMAIL_EMAIL; // O e-mail que você verificou no SendGrid

    if (!process.env.SENDGRID_API_KEY || !fromEmail) {
        logger.error("SENDGRID_API_KEY ou GMAIL_EMAIL não encontrados nos secrets.");
        return;
    }

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

    const numeroPedido = novoPedido.numero_pedido || "Sem Número";
    const valorPedido = novoPedido.valor.toFixed(2).replace(".", ",");

    try {
        const requisicaoDoc = await getFirestore().collection("requisicoes").doc(novoPedido.requisicao_id).get();
        const requisicaoData = requisicaoDoc.exists ? requisicaoDoc.data() : {};
        const demandaDoc = requisicaoData.demanda_id ? await getFirestore().collection("demandas").doc(requisicaoData.demanda_id).get() : null;
        const demandaData = demandaDoc && demandaDoc.exists ? demandaDoc.data() : {};

        const descricaoDemanda = demandaData.descricao_necessidade || "Descrição não encontrada";
        const solicitanteUsername = demandaData.solicitante_demanda || "Solicitante não encontrado";

        // Monta a mensagem para o SendGrid
        const msg = {
            to: emailList, // SendGrid aceita um array de e-mails diretamente!
            from: {
                name: "Sistema de Compras",
                email: fromEmail // Use o e-mail verificado
            },
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
            <p><em>Esta é uma mensagem automática.</em></p>
          `,
          attachments: []
        };

        if (novoPedido.anexo_email && novoPedido.anexo_email.b64_data) {
            msg.attachments.push({
                content: novoPedido.anexo_email.b64_data,
                filename: novoPedido.anexo_email.file_name,
                type: novoPedido.anexo_email.content_type,
                disposition: 'attachment'
            });
        }

        await sgMail.send(msg);
        logger.log(`E-mail enviado via SendGrid para: ${emailList.join(", ")}`);

    } catch (error) {
        logger.error("Erro ao enviar e-mail via SendGrid:", error.response ? error.response.body : error);
    }
});