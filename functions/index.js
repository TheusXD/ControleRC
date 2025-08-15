/**
 * Este código utiliza a sintaxe V2 do Firebase Functions, que é a mais moderna.
 */
const {onDocumentCreated} = require("firebase-functions/v2/firestore");
const {initializeApp} = require("firebase-admin/app");
const {getFirestore} = require("firebase-admin/firestore");
const nodemailer = require("nodemailer");
const logger = require("firebase-functions/logger");

// **CORREÇÃO:** Carrega as variáveis de ambiente do arquivo .env.production
require("dotenv").config({ path: '.env.production' });

// Inicializa o app do Firebase Admin
initializeApp();

// --- Configuração do Transportador de E-mail ---
// As credenciais de e-mail agora são lidas das variáveis de ambiente
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
    // 1. Obter os dados do novo pedido que foi criado.
    const snap = event.data;
    if (!snap) {
        logger.log("Nenhum dado associado ao evento.");
        return;
    }
    const novoPedido = snap.data();
    const numeroPedido = novoPedido.numero_pedido || "Sem Número";
    const valorPedido = novoPedido.valor.toFixed(2).replace(".", ",");

    try {
        // 2. Buscar a Requisição (RC) vinculada ao pedido para encontrar a demanda.
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

        // 3. Buscar a Demanda original para encontrar o solicitante.
        const demandaDoc = await getFirestore()
            .collection("demandas")
            .doc(demandaId)
            .get();
        if (!demandaDoc.exists) {
            logger.log(`Demanda com ID ${demandaId} não encontrada.`);
            return;
        }
        const demandaData = demandaDoc.data();
        const solicitanteUsername = demandaData.solicitante_demanda;
        const descricaoDemanda = demandaData.descricao_necessidade;

        // 4. Buscar o usuário (gestor) no banco de dados para obter o e-mail.
        const usersRef = getFirestore().collection("users");
        const userQuery = await usersRef.where("username", "==", solicitanteUsername).limit(1).get();

        if (userQuery.empty) {
            logger.log(`Usuário ${solicitanteUsername} não encontrado para notificação.`);
            return;
        }
        const userData = userQuery.docs[0].data();
        const emailGestor = userData.email;

        if (!emailGestor) {
            logger.log(`Usuário ${solicitanteUsername} não possui um e-mail cadastrado.`);
            return;
        }

        // 5. Montar e enviar o e-mail de notificação.
        const mailOptions = {
            from: `"Sistema de Compras" <${gmailEmail}>`,
            to: emailGestor,
            subject: `✅ Novo Pedido Gerado para sua Demanda: ${numeroPedido}`,
            html: `
            <p>Olá, ${solicitanteUsername}!</p>
            <p>Um novo pedido de compra foi gerado a partir de uma de suas demandas.</p>
            <hr>
            <h3>Detalhes do Pedido</h3>
            <ul>
              <li><strong>Número do Pedido:</strong> ${numeroPedido}</li>
              <li><strong>Valor:</strong> R$ ${valorPedido}</li>
            </ul>
            <h3>Demanda Original</h3>
            <p><strong>Descrição:</strong> ${descricaoDemanda}</p>
            <hr>
            <p>Você pode acompanhar o status do pedido através do sistema.</p>
            <p><em>Esta é uma mensagem automática, por favor, não responda.</em></p>
          `,
        };

        await mailTransport.sendMail(mailOptions);
        logger.log(`Notificação enviada com sucesso para ${emailGestor}`);

    } catch (error) {
        logger.error("Erro ao enviar notificação por e-mail:", error);
    }
});
