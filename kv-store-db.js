import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';

dotenv.config();

// Configuração do Supabase
const supabaseUrl = process.env.SUPABASE_URL || 'https://saeuypetnnvvkilqquye.supabase.co';
const supabaseKey = process.env.SUPABASE_ANON_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNhZXV5cGV0bm52dmtpbHFxdXllIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTc1MjU0OTAsImV4cCI6MjA3MzEwMTQ5MH0.TIaLYP59KmpAnZKOF_qxfSySGvXncWIUCEeN0xGJ_gs';

// Criar cliente Supabase
const supabase = createClient(supabaseUrl, supabaseKey);

const TABLE_NAME = 'kv_store_15406bac';

/**
 * Insere dados na tabela kv_store usando signature como key
 * @param {Object} data - Dados da transação
 * @returns {Promise<Object>} - Resultado da inserção
 */
async function insertTransaction(data) {
  try {
    // Usar signature como key
    const key = data.signature;
    
    // Preparar dados apenas para as colunas que existem na tabela
    const mainData = {
      key: key,
      signature: data.signature,
      source_url: data.source_url || null,
      timestamp: data.timestamp || null,
      mint: data.mint || null,
      to_user_account: data.to_user_account || null,
      token_amount: data.token_amount || null,
      token_name: data.token_name || null,
      token_symbol: data.token_symbol || null,
      token_icon: data.token_icon || null,
      dev: data.dev || null,
      first_pool_created_at: data.first_pool_created_at || null,
      holder_count: data.holder_count || null,
      mcap: data.mcap || null
    };

    // Tentar inserir (UPSERT)
    const { data: result, error } = await supabase
      .from(TABLE_NAME)
      .upsert([mainData], { 
        onConflict: 'key',
        ignoreDuplicates: false 
      })
      .select();

    if (error) {
      console.error(`❌ Error inserting transaction ${data.signature}:`, error.message);
      throw error;
    }

    console.log(`✅ Upserted transaction: ${data.signature}`);
    return { status: 'inserted', signature: data.signature, data: result[0] };

  } catch (error) {
    console.error(`❌ Error processing transaction ${data.signature}:`, error.message);
    throw error;
  }
}

/**
 * Insere múltiplas transações em lote
 * @param {Array} dataArray - Array de dados de transações
 * @returns {Promise<Object>} - Resultado do lote
 */
async function insertTransactionBatch(dataArray) {
  try {
    // Remover duplicatas baseado na signature
    const uniqueData = [];
    const seenSignatures = new Set();
    
    for (const data of dataArray) {
      if (!seenSignatures.has(data.signature)) {
        seenSignatures.add(data.signature);
        uniqueData.push(data);
      } else {
        console.log(`⚠️ Duplicate signature removed from batch: ${data.signature}`);
      }
    }

    if (uniqueData.length !== dataArray.length) {
      console.log(`🔄 Removed ${dataArray.length - uniqueData.length} duplicate signatures from batch`);
    }

    // Preparar dados para o lote
    const batchData = uniqueData.map(data => {
      const key = data.signature;
      
      // Apenas as colunas que existem na tabela
      const mainData = {
        key: key,
        signature: data.signature,
        source_url: data.source_url || null,
        timestamp: data.timestamp || null,
        mint: data.mint || null,
        to_user_account: data.to_user_account || null,
        token_amount: data.token_amount || null,
        token_name: data.token_name || null,
        token_symbol: data.token_symbol || null,
        token_icon: data.token_icon || null,
        dev: data.dev || null,
        first_pool_created_at: data.first_pool_created_at || null,
        holder_count: data.holder_count || null,
        mcap: data.mcap || null,
        twitter: data.twitter || null,
        website: data.website || null
      };

      return mainData;
    });

    const { data: result, error } = await supabase
      .from(TABLE_NAME)
      .upsert(batchData, { 
        onConflict: 'key',
        ignoreDuplicates: false 
      })
      .select();

    if (error) {
      console.error('Batch upsert error:', error);
      // Se houver erro no lote, tentar inserir um por um usando dados únicos
      return await insertTransactionsOneByOne(uniqueData);
    }

    console.log(`✅ Batch upserted ${result.length} transactions`);
    return {
      inserted: result.length,
      skipped: dataArray.length - uniqueData.length, // Contar duplicatas como skipped
      failed: 0,
      results: result.map(item => ({ status: 'inserted', signature: item.signature }))
    };

  } catch (error) {
    console.error('❌ Batch upsert failed:', error.message);
    // Fallback para inserção individual usando dados únicos
    return await insertTransactionsOneByOne(uniqueData);
  }
}

/**
 * Inserir transações uma por uma (fallback)
 * @param {Array} dataArray - Array de dados
 * @returns {Promise<Object>} - Resultado
 */
async function insertTransactionsOneByOne(dataArray) {
  let inserted = 0, skipped = 0, failed = 0;
  const results = [];

  for (const data of dataArray) {
    try {
      const result = await insertTransaction(data);
      if (result.status === 'inserted') inserted++;
      else if (result.status === 'skipped') skipped++;
      results.push(result);
    } catch (error) {
      failed++;
      results.push({ status: 'failed', signature: data.signature, error: error.message });
    }
  }

  return { inserted, skipped, failed, results };
}

/**
 * Busca uma transação por signature (key)
 * @param {string} signature - Signature da transação
 * @returns {Promise<Object|null>} - Dados da transação ou null
 */
async function getBySignature(signature) {
  try {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('*')
      .eq('key', signature)
      .single();

    if (error) {
      if (error.code === 'PGRST116') { // Not found
        return null;
      }
      throw error;
    }

    return data;
  } catch (error) {
    console.error(`Error fetching signature ${signature}:`, error.message);
    return null;
  }
}

/**
 * Verifica quais signatures já existem no banco
 * @param {Array} signatures - Array de signatures
 * @returns {Promise<Set>} - Set com signatures existentes
 */
async function getExistingSignatures(signatures) {
  if (!signatures || signatures.length === 0) return new Set();

  try {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('key')
      .in('key', signatures);

    if (error) {
      console.error('Error checking existing signatures:', error);
      return new Set(); // Retorna set vazio em caso de erro
    }

    return new Set(data.map(row => row.key));
  } catch (error) {
    console.error('Error checking existing signatures:', error.message);
    return new Set();
  }
}

/**
 * Busca transações com filtros e paginação
 * @param {Object} options - Opções de busca
 * @returns {Promise<Object>} - Resultado da busca
 */
async function getTransactions(options = {}) {
  try {
    let query = supabase.from(TABLE_NAME).select('*');

    // Aplicar filtros
    if (options.mint) {
      query = query.eq('mint', options.mint);
    }
    if (options.signature) {
      query = query.eq('key', options.signature);
    }
    if (options.fromTimestamp) {
      query = query.gte('timestamp', options.fromTimestamp);
    }
    if (options.toTimestamp) {
      query = query.lte('timestamp', options.toTimestamp);
    }
    if (options.token_symbol) {
      query = query.eq('token_symbol', options.token_symbol);
    }

    // Ordenação
    if (options.orderBy) {
      query = query.order(options.orderBy, { ascending: options.ascending !== false });
    } else {
      query = query.order('timestamp', { ascending: false });
    }

    // Paginação
    if (options.limit) {
      query = query.limit(options.limit);
    }
    if (options.offset) {
      query = query.range(options.offset, options.offset + (options.limit || 100) - 1);
    }

    const { data, error, count } = await query;

    if (error) {
      throw error;
    }

    return { data, count };
  } catch (error) {
    console.error('Error fetching transactions:', error.message);
    throw error;
  }
}

/**
 * Testa a conexão com o Supabase
 * @returns {Promise<boolean>} - True se conectado com sucesso
 */
async function testConnection() {
  try {
    const { data, error } = await supabase
      .from(TABLE_NAME)
      .select('count')
      .limit(1);

    if (error) {
      console.error('Supabase connection test failed:', error);
      return false;
    }

    console.log('✅ Supabase connection successful');
    return true;
  } catch (error) {
    console.error('❌ Supabase connection failed:', error.message);
    return false;
  }
}

export {
  insertTransaction,
  insertTransactionBatch,
  getBySignature,
  getExistingSignatures,
  getTransactions,
  testConnection,
  supabase // Exportar cliente para uso direto se necessário
};
