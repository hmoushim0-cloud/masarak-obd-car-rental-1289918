package sa.factory.masarakobdcarrenta.data.repository

import sa.factory.masarakobdcarrenta.domain.repository.MasarakObdRepository
import sa.factory.masarakobdcarrenta.hardware.HardwareSensorDriver
import sa.factory.masarakobdcarrenta.model.MasarakObdItem
import sa.factory.masarakobdcarrenta.model.MasarakObdOrder
import sa.factory.masarakobdcarrenta.model.MasarakObdTelemetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch

class MasarakObdRepositoryImpl(
    private val sensorDriver: HardwareSensorDriver
) : MasarakObdRepository {

    private val _catalog = MutableStateFlow<List<MasarakObdItem>>(listOf(
        MasarakObdItem("1", "مزامنة أساسية", "مزامنة يومية مجدولة عبر rclone مع تشفير AES-256 لبيانات المركبات", 150.0, "معتمد", "مزامنة تلقائية"),
        MasarakObdItem("2", "مزامنة متقدمة", "مزامنة فورية عند توصيل OBD-II مع تحليل استهلاك الوقود", 280.0, "فوري", "حفظ احتياطي سحابي"),
        MasarakObdItem("3", "مزامنة فورية", "نسخ احتياطي عبر BLE عند اقتراب المفتاح الرقمي من المركبة", 95.0, "جديد", "تتبع الأسطول")
    ))
    override val catalogFlow: Flow<List<MasarakObdItem>> = _catalog.asStateFlow()

    private val _cart = MutableStateFlow<List<MasarakObdItem>>(emptyList())
    override val activeCart: StateFlow<List<MasarakObdItem>> = _cart.asStateFlow()

    override val telemetryFlow: Flow<MasarakObdTelemetry> = sensorDriver.observePeripheralStream()
        .map { frame ->
            MasarakObdTelemetry(
                peripheralName = frame.peripheralName,
                status = frame.connectionState,
                liveReading = frame.livePayload,
                batteryPct = 96,
                signalDbm = -54
            )
        }

    override fun addToCart(item: MasarakObdItem) {
        val current = _cart.value.toMutableList()
        current.add(item)
        _cart.value = current
    }

    override fun removeFromCart(item: MasarakObdItem) {
        val current = _cart.value.toMutableList()
        current.remove(item)
        _cart.value = current
    }

    override fun clearCart() {
        _cart.value = emptyList()
    }

    override suspend fun checkout(paymentMethod: String): MasarakObdOrder {
        val currentItems = _cart.value.toList()
        val subtotal = currentItems.sumOf { it.priceSar }
        val vat = subtotal * 0.15
        val total = subtotal + vat
        val order = MasarakObdOrder(
            orderId = "ORD-${System.currentTimeMillis() % 100000}",
            items = currentItems,
            subtotalSar = subtotal,
            vatSar = vat,
            totalSar = total,
            paymentMethod = paymentMethod
        )
        clearCart()
        return order
    }

    override suspend fun refreshCatalog() {
        // Offline-first sync logic
    }
}
