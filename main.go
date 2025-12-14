package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// --- ESTRUTURAS ---
type Mensagem struct {
	Conteudo  string `json:"conteudo"`
	Timestamp int    `json:"timestamp"`
	ProcessID int    `json:"process_id"` // quem está enviando AGORA
	Tipo      string `json:"tipo"`
	MaiorID   int    `json:"maior_id"`  // maior ID encontrado
	OrigemID  int    `json:"origem_id"` // quem iniciou a eleição
}

// --- VARIÁVEIS GLOBAIS ---
var (
	meuID         int
	relogioLogico int
	mutex         sync.Mutex
	fila          []Mensagem
	// Tabela Hash para a contagem de acks. Chave: "TS_ID", Valor: Qtd Acks
	acks         = make(map[string]int)
	requiredAcks = 2
	peers        = []string{"http://proc1:8080", "http://proc2:8080", "http://proc3:8080"}

	// --- Q2: TOKEN RING ---
	temToken         bool
	processoSucessor int
	emSecaoCritica   bool

	// --- Q3: ELEIÇÃO DE LÍDER ---
	liderAtual   int
	eleicaoAtiva bool
)

// --- LÓGICA DO RELÓGIO (Lamport) ---

func atualizarRelogio(recebido int) {
	mutex.Lock()
	defer mutex.Unlock()
	if recebido > relogioLogico {
		relogioLogico = recebido
	}
	relogioLogico++
}

func incrementarRelogio() int {
	mutex.Lock()
	defer mutex.Unlock()
	relogioLogico++
	return relogioLogico
}

// --- FUNÇÃO AUXILIAR DE REDE ---

func sendRequest(url string, msg Mensagem) error {
	msgJSON, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("erro ao serializar JSON: %w", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(msgJSON))
	if err != nil {
		return fmt.Errorf("erro ao enviar POST para %s: %v", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("erro: %s respondeu com status %d", url, resp.StatusCode)
	}
	return nil
}

// --- Q1: ORDENAÇÃO TOTAL (Multicast + Lamport) ---

func tentarProcessarMensagens() {
	for {
		mutex.Lock()
		if len(fila) == 0 {
			mutex.Unlock()
			return
		}

		msg := fila[0]
		key := fmt.Sprintf("%d_%d", msg.Timestamp, msg.ProcessID)

		if acks[key] >= requiredAcks {
			fmt.Printf("\n[PROCESSADO] MSG T.O. -> TS: %d, ID: %d, Conteúdo: %s\n", msg.Timestamp, msg.ProcessID, msg.Conteudo)
			fila = fila[1:]
			delete(acks, key)
			mutex.Unlock()
			continue
		}
		mutex.Unlock()
		return
	}
}

func receiveMessageInternal(msg Mensagem) {
	atualizarRelogio(msg.Timestamp)

	mutex.Lock()

	fila = append(fila, msg)

	sort.Slice(fila, func(i, j int) bool {
		if fila[i].Timestamp != fila[j].Timestamp {
			return fila[i].Timestamp < fila[j].Timestamp
		}
		return fila[i].ProcessID < fila[j].ProcessID
	})

	key := fmt.Sprintf("%d_%d", msg.Timestamp, msg.ProcessID)
	acks[key] = 1

	mutex.Unlock()

	fmt.Printf("[RECEBIDO] MSG -> ID: %d, Conteúdo: %s. Fila agora: %d itens.\n", msg.ProcessID, msg.Conteudo, len(fila))

	tentarProcessarMensagens()
}

// Handler para receber a mensagem de multicast de outro processo
func receiveMessage(c *gin.Context) {
	var msg Mensagem
	if err := c.BindJSON(&msg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	receiveMessageInternal(msg)

	// Envia ACK de volta ao remetente
	ackMsg := Mensagem{
		Conteudo:  "ACK",
		Timestamp: incrementarRelogio(),
		ProcessID: meuID,
		Tipo:      "ACK",
		// Usa MaiorID para carregar o TS da MSG original, que é necessário para a chave do ACK.
		MaiorID: msg.Timestamp,
	}

	remetenteURL := fmt.Sprintf("http://proc%d:8080/ack", msg.ProcessID)
	go func() {
		if err := sendRequest(remetenteURL, ackMsg); err != nil {
			fmt.Printf("ERRO: Falha ao enviar ACK para %s: %v\n", remetenteURL, err)
		} else {
			fmt.Printf("-> ACK enviado para Processo %d. TS: %d\n", msg.ProcessID, ackMsg.Timestamp)
		}
	}()

	c.JSON(http.StatusOK, gin.H{"status": "message_received"})
}

// Handler para receber o ACK de um peer
func receiveACK(c *gin.Context) {
	var ackMsg Mensagem
	if err := c.BindJSON(&ackMsg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	atualizarRelogio(ackMsg.Timestamp)

	// TS da mensagem ORIGINAL que este ACK confirma (armazenado em ackMsg.MaiorID)
	msgTSOriginal := ackMsg.MaiorID

	// A chave é a mensagem original
	key := fmt.Sprintf("%d_%d", msgTSOriginal, ackMsg.ProcessID)

	mutex.Lock()
	if _, ok := acks[key]; ok {
		acks[key]++
		fmt.Printf("[ACK RECEBIDO] Msg %s. Total: %d\n", key, acks[key])
	}
	mutex.Unlock()

	tentarProcessarMensagens()

	c.JSON(http.StatusOK, gin.H{"status": "ack_received"})
}

// Dispara o multicast para todos os peers (Q1)
func dispararMulticast(conteudo string) Mensagem {
	msg := Mensagem{
		Conteudo:  conteudo,
		Timestamp: incrementarRelogio(),
		ProcessID: meuID,
		Tipo:      "MESSAGE",
	}

	receiveMessageInternal(msg)

	for _, peerURL := range peers {
		if peerURL != fmt.Sprintf("http://proc%d:8080", meuID) {
			go func(url string) {
				fmt.Printf("-> Enviando Multicast para %s: Conteúdo: '%s' TS: %d\n", url, msg.Conteudo, msg.Timestamp)
				if err := sendRequest(url+"/receive", msg); err != nil {
					fmt.Printf("ERRO: Falha ao enviar multicast para %s: %v\n", url, err)
				}
			}(peerURL)
		}
	}
	return msg
}

// --- Q2: EXCLUSÃO MÚTUA (TOKEN RING) ---

func passarToken() {
	tokenMsg := Mensagem{
		Conteudo:  "TOKEN",
		Timestamp: incrementarRelogio(),
		ProcessID: meuID,
		Tipo:      "TOKEN",
	}

	sucessorURL := fmt.Sprintf("http://proc%d:8080/token", processoSucessor)

	go func() {
		fmt.Printf("-> Passando TOKEN para Processo %d...\n", processoSucessor)
		if err := sendRequest(sucessorURL, tokenMsg); err != nil {
			fmt.Printf("ERRO: Falha ao passar TOKEN para %s: %v\n", sucessorURL, err)
		} else {
			mutex.Lock()
			temToken = false
			mutex.Unlock()
			fmt.Println("-> TOKEN entregue com sucesso.")
		}
	}()
}

func processarSC() {
	mutex.Lock()
	if !temToken || emSecaoCritica {
		mutex.Unlock()
		return
	}
	emSecaoCritica = true
	mutex.Unlock()

	fmt.Println("\n=============================================")
	fmt.Printf("ENTRANDO na Seção Crítica (SC) - TS: %d\n", incrementarRelogio())
	time.Sleep(3 * time.Second)
	fmt.Printf("SAINDO da Seção Crítica (SC) - TS: %d\n", incrementarRelogio())
	fmt.Println("=============================================\n")

	mutex.Lock()
	emSecaoCritica = false
	mutex.Unlock()

	passarToken()
}

// Handler de Token (Q2)
func receiveToken(c *gin.Context) {
	var msg Mensagem
	c.BindJSON(&msg)
	atualizarRelogio(msg.Timestamp)

	mutex.Lock()
	if temToken {
		fmt.Println("[WARNING] Recebendo um token extra. Ignorando.")
		mutex.Unlock()
		c.JSON(http.StatusOK, gin.H{"status": "token_ignorado"})
		return
	}
	temToken = true
	mutex.Unlock()

	fmt.Printf("[TOKEN RECEBIDO] Processo %d agora possui o token.\n", meuID)

	processarSC()

	c.JSON(http.StatusOK, gin.H{"status": "token_ok"})
}

// Handler de Requisição de SC (Q2)
func requestSC(c *gin.Context) {
	fmt.Println("\n[REQUISIÇÃO SC] Recebida requisição do usuário.")

	if temToken {
		fmt.Println("[REQUISIÇÃO SC] Possui o token, entrando na SC.")
		processarSC()
	} else {
		fmt.Println("[REQUISIÇÃO SC] Não possui o token. Aguardando a chegada do token...")
	}

	c.JSON(http.StatusOK, gin.H{"status": "tentativa_sc_iniciada", "possui_token": temToken})
}

// --- Q3: ELEIÇÃO DE LÍDER (ANEL com COOR) ---

// Dispara o anúncio do líder eleito para encerrar a eleição nos peers
func announceCoordinator(liderID int) {

	msgCoord := Mensagem{
		Conteudo:  "COOR",
		Timestamp: incrementarRelogio(),
		ProcessID: liderID,
		Tipo:      "COOR",
	}

	sucessorURL := fmt.Sprintf("http://proc%d:8080/coordinator", processoSucessor)

	go func() {
		fmt.Printf("-> ANUNCIANDO NOVO LÍDER (%d) para Processo %d...\n", liderID, processoSucessor)
		if err := sendRequest(sucessorURL, msgCoord); err != nil {
			fmt.Printf("ERRO: Falha ao anunciar COOR para %s: %v\n", sucessorURL, err)
		}
	}()
}

func propagarEleicao(maiorIDAtual int) {
	mutex.Lock()
	if !eleicaoAtiva {
		mutex.Unlock()
		return
	}
	mutex.Unlock()

	// O maior ID atual é propagado para o sucessor
	msgEleicao := Mensagem{
		Conteudo:  "ELEICAO",
		Timestamp: incrementarRelogio(),
		ProcessID: meuID,
		Tipo:      "ELEICAO",
		MaiorID:   maiorIDAtual,
	}

	sucessorURL := fmt.Sprintf("http://proc%d:8080/eleicao", processoSucessor)

	go func() {
		fmt.Printf("-> Propagando ELEIÇÃO (Líder Provisório: %d) para Processo %d...\n", maiorIDAtual, processoSucessor)
		if err := sendRequest(sucessorURL, msgEleicao); err != nil {
			fmt.Printf("ERRO: Falha ao propagar ELEIÇÃO para %s: %v\n", sucessorURL, err)
		}
	}()
}

// Função que inicia a eleição
func iniciarEleicao() {
	mutex.Lock()
	if eleicaoAtiva {
		mutex.Unlock()
		return
	}
	eleicaoAtiva = true
	mutex.Unlock()

	fmt.Printf("\n[ELEIÇÃO] Processo %d iniciou a eleição\n", meuID)

	msg := Mensagem{
		Tipo:     "ELEICAO",
		OrigemID: meuID,
		MaiorID:  meuID,
	}

	url := fmt.Sprintf("http://proc%d:8080/eleicao", processoSucessor)
	go sendRequest(url, msg)
}

// Handler de Eleição (Q3)
func receiveEleicao(c *gin.Context) {
	var msg Mensagem
	c.BindJSON(&msg)

	// Atualiza maior ID
	if meuID > msg.MaiorID {
		msg.MaiorID = meuID
	}

	// CONDIÇÃO DE PARADA
	if msg.OrigemID == meuID {
		mutex.Lock()
		liderAtual = msg.MaiorID
		eleicaoAtiva = false
		mutex.Unlock()

		fmt.Printf("\n[ELEIÇÃO FINALIZADA] Líder eleito: Processo %d\n\n", liderAtual)
		announceCoordinator(liderAtual)
		c.JSON(200, gin.H{"status": "eleicao_finalizada"})
		return
	}

	// Propaga normalmente
	msg.ProcessID = meuID
	url := fmt.Sprintf("http://proc%d:8080/eleicao", processoSucessor)
	go sendRequest(url, msg)

	c.JSON(200, gin.H{"status": "eleicao_propagada"})
}

// Handler COOR: Recebe o novo líder (Q3)
func receiveCoordinator(c *gin.Context) {
	var msg Mensagem
	c.BindJSON(&msg)

	mutex.Lock()
	if !eleicaoAtiva && liderAtual == msg.ProcessID {
		mutex.Unlock()
		c.JSON(200, gin.H{"status": "coor_ignorado"})
		return
	}

	eleicaoAtiva = false
	liderAtual = msg.ProcessID
	mutex.Unlock()

	fmt.Printf("[COORDENADOR] Novo líder confirmado: Processo %d\n", liderAtual)

	// Propaga apenas uma vez
	if meuID != msg.ProcessID {
		url := fmt.Sprintf("http://proc%d:8080/coordinator", processoSucessor)
		go sendRequest(url, msg)
	}

	c.JSON(200, gin.H{"status": "coordinator_ok"})
}

// --- HANDLERS DIVERSOS ---

// Handler de teste (Q1)
func startInternalTest(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// Handler para o usuário disparar uma mensagem de multicast (Q1)
func startMulticast(c *gin.Context) {
	var req struct {
		Conteudo string `json:"conteudo"`
	}
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing 'conteudo' in request"})
		return
	}

	msg := dispararMulticast(req.Conteudo)
	c.JSON(http.StatusOK, gin.H{"status": "multicast_started", "message": msg})
}

// Handler para o usuário disparar a eleição (Q3)
func startEleicao(c *gin.Context) {
	go iniciarEleicao()
	c.JSON(http.StatusOK, gin.H{"status": "eleicao_iniciada"})
}

// --- MAIN ---
func main() {
	idStr := os.Getenv("MY_ID")
	if idStr == "" {
		idStr = "1"
	}
	meuID, _ = strconv.Atoi(idStr)

	relogioLogico = 0

	// --- Q2: INICIALIZAÇÃO DO TOKEN RING ---
	if meuID == 1 {
		temToken = true
		fmt.Println("--- Processo 1: TOKEN INICIALIZADO ---")
	} else {
		temToken = false
	}
	processoSucessor = (meuID % 3) + 1
	emSecaoCritica = false

	// --- Q3: INICIALIZAÇÃO DA ELEIÇÃO ---
	liderAtual = 1
	eleicaoAtiva = false

	gin.DisableBindValidation()
	router := gin.New()
	router.Use(gin.Logger())
	router.Use(gin.RecoveryWithWriter(gin.DefaultErrorWriter))

	// Endpoints da Q1 (Ordenação Total)
	router.POST("/receive", receiveMessage)
	router.POST("/ack", receiveACK)
	router.POST("/start", startMulticast)
	router.GET("/test", startInternalTest)

	// Endpoints da Q2 (Token Ring)
	router.POST("/token", receiveToken)
	router.POST("/request_sc", requestSC)

	// Endpoints da Q3 (Eleição de Líder)
	router.POST("/eleicao", receiveEleicao)
	router.POST("/start_eleicao", startEleicao)
	router.POST("/coordinator", receiveCoordinator) // Nova Rota COOR

	fmt.Printf("=== Processo %d rodando | Sucessor: %d | Lider Inicial: %d ===\n", meuID, processoSucessor, liderAtual)
	router.Run(":8080")
}
