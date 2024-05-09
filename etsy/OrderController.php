<?php

namespace App\Http\Controllers;

use App\Http\Requests\OrderDetailRequest;
use App\Http\Requests\ProductSupplierRequest;
use App\Http\Resources\AgGridResource;
use App\Http\Traits\FullDesignTrait;
use App\Http\Traits\LineItemTrait;
use App\Http\Traits\OrderSupplierTrait;
use App\Http\Traits\Supplier\PushOrderSupplierTrait;
use App\Http\Traits\Supplier\SupplierTrait;
use App\Http\Traits\TeamAccountTrait;
use App\Http\Traits\TransactionDesignTrait;
use App\Jobs\PushOrderToElasticJob;
use App\Jobs\SaveReceipt;
use App\Jobs\SyncReceipt;
use App\Library\AwsS3Store;
use App\Library\CdnUrl;
use App\Library\ElasticSearchApi;
use App\Library\EtsyV3Oauth;
use App\Library\Helper;
use App\Library\LarkMessageApi;
use App\Library\SmartyApi;
use App\Model\Design;
use App\Model\Listing;
use App\Model\ListingHistory;
use App\Model\Receipt;
use App\Model\Supplier;
use App\Model\SupplierVariant;
use App\Model\Transaction;
use App\Model\User;
use App\Rules\ShippingService;
use App\Services\LogIgnore;
use Artisan;
use Carbon\Carbon;
use DB;
use Exception;
use Illuminate\Database\Query\Builder;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\App;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Redis;
use Illuminate\Support\Facades\Validator;
use Illuminate\Support\Str;
use Throwable;

class OrderController extends Controller
{
    use LineItemTrait;
    use FullDesignTrait;
    use OrderSupplierTrait;
    use TeamAccountTrait;
    use TransactionDesignTrait;
    use PushOrderSupplierTrait;
    use SupplierTrait;

    const ELASTIC_INDEX_ORDERS = 'orders';
    const ELASTIC_MAX_RESULT_WINDOW = 50000;

    const US_LOS_ANGELES_TZ = 'GMT-8';

    protected int $elasticEnable;
    protected ElasticSearchApi $elastic;

    public function __construct()
    {
        $this->elasticEnable = intval(env('ELASTIC_ENABLE', 0));
        $this->elastic = new ElasticSearchApi();
    }

    public function index(Request $request)
    {
        if ($this->elasticEnable === 1) {
            $days = intval(env('ELASTIC_DATE_QUERY', 60));

            $from = intval($request->get('startRow'));
            $size = 100;

            if ($from + $size > self::ELASTIC_MAX_RESULT_WINDOW) {
                Log::error('Elastic search index from + size > ' . self::ELASTIC_MAX_RESULT_WINDOW);
            }

            $filters = [
                "must" => [],
            ];

            if ($request->has('q')) {
                $filter_condition = [
                    'wildcard' => [
                        'id' => $request->get('q') . '*'
                    ]
                ];
                array_push($filters['must'], $filter_condition);
            }

            if ($request->has('full_design')) {
                if ('isDead' == $request->get('full_design')) {
                    $filter_condition = [
                        'match' => [
                            'is_dead' => 1
                        ]
                    ];

                    array_push($filters['must'], $filter_condition);

                } else {
                    $filter_condition = [
                        'match' => [
                            'full_design' => $request->get('full_design')
                        ]
                    ];

                    array_push($filters['must'], $filter_condition);
                }
            }

//            if ($request->has('supplier_status')) {
//                $filter_condition = [
//                    'match' => [
//                        'line_items' => $request->get('supplier_status'),
//                    ]
//                ];
//
//                array_push($filters['must'], $filter_condition);
//            }

//            if ($request->has('tracking_status')) {
//                $filter_condition = [
//                    'match' => [
//                        'line_items' => $request->get('tracking_status'),
//                    ]
//                ];
//
//                array_push($filters['must'], $filter_condition);
//            }

            if ($request->has('tracking_status') && !in_array($request->tracking_status, ['pending', 'is_overdue'])) {
                $filter_isdead = [
                    'match' => [
                        'is_dead' => 0,
                    ]
                ];
                array_push($filters['must'], $filter_isdead);

                $filter_account_status = [
                    'match' => [
                        'account_status' => 'active',
                    ]
                ];
                array_push($filters['must'], $filter_account_status);

                $filter_tracking_status = [
                    'match' => [
                        'line_items' => $request->get('tracking_status')
                    ]
                ];
                array_push($filters['must'], $filter_tracking_status);
            }
            if ($request->has('supplier_status') || 'pending' == $request->tracking_status) {
                $filter_isdead = [
                    'match' => [
                        'is_dead' => 0,
                    ]
                ];
                array_push($filters['must'], $filter_isdead);

                $filter_account_status = [
                    'match' => [
                        'account_status' => 'active',
                    ]
                ];
                array_push($filters['must'], $filter_account_status);

                if ('pending' == $request->tracking_status) {
                    $request->supplier_status = 'submitted';
                }

                $filter_supplier_status = [
                    'match' => [
                        'line_items' => $request->get('supplier_status')
                    ]
                ];

                array_push($filters['must'], $filter_supplier_status);
            }

            if ($request->has('shop_id')) {
                $filter['match'] = [
                    'shop_id' => $request->get('shop_id'),
                ];
            }

//            if ($request->has('fromDate') && $request->has('toDate'))
//            {
//                $filter['range'] = [
//                    'created_at' => [
//                        'gte' => $request->get('fromDate'),
//                        'lte' => $request->get('toDate'),
//                    ],
//                ];
//            } else {
//                $filter['range'] = [
//                    'created_at' => [
//                        'gte' => 'now-' . $days . 'd/d',
//                        'lte' => 'now/d',
//                    ],
//                ];
//            }

//            $accountIds = $this->getAccountList() ?? [];
//
//            $filter[] = [
//                'terms' => [
//                    'account_id' => $accountIds,
//                ],
//            ];

//            $query = DB::table('receipts')
//                ->join('accounts', 'receipts.seller_user_id', '=', 'accounts.id')
//                ->join('shops', 'shops.account_id', '=', 'accounts.id')
//                ->leftJoin('receipt_shipments', 'receipts.id', '=', 'receipt_shipments.receipt_id')
//                ->leftJoin('currency_rates', 'receipts.currency_code', '=', 'currency_rates.currency_code')
//                ->where('receipts.was_paid', '=', '1');
//
//            $query = $query->whereNull('receipts.deleted_at');
//
//            if ($request->get('fromDate')) {
//                $query = $query->whereDate('receipts.creation_tsz', '>=', Carbon::parse($request->get('fromDate') . '00:00:00'));
//            } else {
//                $query = $query->whereDate('receipts.creation_tsz', '>=', Carbon::now()->subDays(60)->toDateTimeString());
//            }
//
//            $query = $query->select(array_merge([
//                'receipts.*',
//                'accounts.nickname',
//                'accounts.status as account_status',
//                'currency_rates.rate',
//                DB::raw('FORMAT(receipts.total_price/currency_rates.rate,2) as total_price'),
//                DB::raw('FORMAT(receipts.grandtotal/currency_rates.rate,2) as grandtotal'),
//                DB::raw('FORMAT(receipts.subtotal/currency_rates.rate,2) as subtotal'),
//                DB::raw('FORMAT(receipts.discount_amt/currency_rates.rate,2) as discount_amt'),
//                DB::raw('FORMAT(receipts.gift_wrap_price/currency_rates.rate,2) as gift_wrap_price'),
//                DB::raw('FORMAT(receipts.total_shipping_cost/currency_rates.rate,2) as total_shipping_cost'),
//                DB::raw('FORMAT(receipts.total_vat_cost/currency_rates.rate,2) as total_vat_cost'),
//                'shops.shop_name',
//                DB::raw('CONCAT(accounts.nickname,"_",shops.shop_name) AS nickname_shop_name'),
//                'receipt_shipments.carrier_name',
//                'receipt_shipments.tracking_code',
//                'receipt_shipments.tracking_url',
//                'receipt_shipments.tracking_status',
//                'receipts.country_iso AS iso_country_code',
//                'receipts.supplier_order_total AS supplier_order_total',
//                'shops.id as shop_id',
//            ], User::selectCompanySupperAdmin($query)));
//
//            $dataOrders = $query->get();
//
//            $checkIndicesExist = $this->elastic->checkIndicesExist('account');
//
//            if ($checkIndicesExist->getReasonPhrase() === 'Not Found') {
//                $this->elastic->createIndices('account');
//            }
//
//            foreach ($dataOrders as $dataOrder) {
//                $arrayOrder = json_decode(json_encode($dataOrder), true);
//                $checkDocumentExit = $this->elastic->checkDocumentExist('account', $arrayOrder['id'])->getReasonPhrase() === 'Not Found';
//
//                if ($checkDocumentExit) {
//                    $this->elastic->createDocument('account', $arrayOrder['id'], $arrayOrder, true);
//                }
//            }

            if (empty($filters)) {
                $response = $this->elastic->searchIndices(
                    'account',
                    0,
                    100,
                    [
                        'query' => [
                            'match_all' => (object) []
                        ],
                    ]
                );
            } else {
                $response = $this->elastic->searchIndices(
                    'account',
                    0,
                    100,
                    [
                        'query' => [
                            'bool' => $filters,
                        ],
                    ]
                );
            }

            $dataResponse = [];
            foreach ($response['hits']['hits'] as $data) {
                array_push($dataResponse, $data['_source']);
            }

            return [
                'lastRow' => $response['hits']['total']['value'],
                'rows' => $dataResponse,
                'filter' => $filters,
            ];

            $hits = [];

            $checkIndicesExist = $this->elastic->checkIndicesExist(self::ELASTIC_INDEX_ORDERS);

            if ($checkIndicesExist->getReasonPhrase() === 'Not Found') {
                $this->elastic->createIndices(self::ELASTIC_INDEX_ORDERS);
            }

            $results = $this->elastic->searchIndices(
                'orders',
                $from,
                $size,
                [
//                    'sort' => [
//                        'created_at' => [
//                            'order' => 'desc',
//                        ],
//                    ],
                    'query' => [
                        'bool' => [
                            'filter' => $filter,
                        ],
                    ],
                ],
            );

            $count = $this->elastic->countIndices(
                self::ELASTIC_INDEX_ORDERS,
                [
                    'query' => [
                        'bool' => [
                            'filter' => $filter
                        ],
                    ],
                ],
            );

            if ($count >= self::ELASTIC_MAX_RESULT_WINDOW) {
                Log::error('Elastic index ' . self::ELASTIC_INDEX_ORDERS . ' > ' . self::ELASTIC_MAX_RESULT_WINDOW);
            }

            foreach ($results['hits']['hits'] as $result) {
                $hits[] = $result['_source'];
            }

            return [
                'rows' => $hits,
                'lastRow' => $count,
                'filter' => $filter,
            ];

        } else {
            $query = DB::table('receipts')
                ->join('accounts', 'receipts.seller_user_id', '=', 'accounts.id')
                ->join('shops', 'shops.account_id', '=', 'accounts.id')
                ->leftJoin('receipt_shipments', 'receipts.id', '=', 'receipt_shipments.receipt_id')
                ->leftJoin('currency_rates', 'receipts.currency_code', '=', 'currency_rates.currency_code')
                ->where('receipts.was_paid', '=', '1');

            $query = $query->whereNull('receipts.deleted_at');
            if (!App::isLocal()) {
                $accountIds = $this->getAccountList() ?? [];
                $query = $query->whereIn('accounts.id', $accountIds);
            }

            if ($request->has('q')) {
                $q = $request->get('q');
                $query->where('receipts.id', 'like', '%' . $q . '%');
//            $query->where(function ($query2) use ($q) {
//                $query2->where('receipts.id', 'like', '%' . $q . '%')
//                    ->orWhere(DB::raw('CONCAT(accounts.nickname,"_",shops.shop_name)'), 'like', '%' . $q . '%')
//                    ->orWhere('accounts.nickname', 'like', '%' . $q . '%')
//                    ->orWhereJsonContains('receipts.line_items', ['to_supplier_order_id' => $q])
//                    ->orWhereJsonContains('receipts.line_items', ['from_supplier_order_id' => $q])
//                    ->orWhereJsonContains('receipts.line_items', ['tracking_code' => $q])
//                    ->orWhere('shops.shop_name', 'like', '%' . $q . '%')
//                    ->orWhere('receipts.buyer_user_id', 'like', '%' . $q . '%')
//                    ->orWhere('receipts.name', 'like', '%' . $q . '%')
//                    ->orWhere('receipts.buyer_email', 'like', '%' . $q . '%')
//                    ->orWhere('receipts.formatted_address', 'like', '%' . $q . '%')
//                ;
//            });
            }

            // start map search order der tracking status
            if ($request->has('tracking_status') && !in_array($request->tracking_status, ['pending', 'is_overdue'])) {
                $status = $request->get('tracking_status');
                $query//->where('receipts.is_shipped', false)
                ->where('receipts.is_dead', false)
                    ->where('accounts.status', 'active');
                if (null == $status) {
                    $query->whereNull('receipts.line_items');
                } else {
                    $query->whereJsonContains('receipts.line_items', ['tracking_status' => $status]);
                }
            }
            if ($request->has('supplier_status') || 'pending' == $request->tracking_status) {
                $query//->where('receipts.is_shipped', false)
                ->where('receipts.is_dead', false)
                    ->where('accounts.status', 'active');
                if ('pending' == $request->tracking_status) {
                    $request->supplier_status = 'submitted';
                } elseif ('new_order' == $request->supplier_status) {
                    $request->supplier_status = null;
                }
                if (null == $request->supplier_status) {
                    $query->where(function ($query2) {
                        $query2->whereJsonLength('receipts.line_items', '<', 1)
                            ->orWhereNull('receipts.line_items');
                    });
                } else {
                    $query->whereJsonContains('receipts.line_items', ['supplier_status' => $request->supplier_status]);
                }
            }

            if ($request->has('overdue_day')) {
                $query->where('receipts.is_shipped', false)
                    ->where('receipts.is_dead', false)
                    ->where('accounts.status', 'active')
                    ->join('transactions', 'transactions.receipt_id', '=', 'receipts.id')
                    ->distinct('receipts.id');
                if ($request->overdue_day < 0) {
                    $query->whereDate('transactions.expected_ship_date', '<', now());
                } elseif ($request->overdue_day < 5) {
                    $query->whereDate('transactions.expected_ship_date', '=', now()->addDays($request->overdue_day - 1)->format('Y-m-d'));
                } else {
                    $query->whereDate('transactions.expected_ship_date', '>=', now()->addDays($request->overdue_day - 1)->format('Y-m-d'));
                }
            }
            if ($request->has('full_design')) {
                if ('isDead' == $request->get('full_design')) {
                    $query->where('receipts.is_dead', 1);
                } else {
                    $query->where('receipts.full_design', $request->get('full_design'));
                }
            }
            // end  map search order der tracking status
            if ($request->has('shop_id')) {
                $query = $query->where('shops.id', '=', $request->get('shop_id'));
            }
            if ($request->get('fromDate')) {
                $query = $query->whereDate('receipts.creation_tsz', '>=', Carbon::parse($request->get('fromDate') . '00:00:00'));
            } else {
                $query = $query->whereDate('receipts.creation_tsz', '>=', Carbon::now()->subDays(60)->toDateTimeString());
            }
            if ($request->get('toDate')) {
                $query = $query->whereDate('receipts.creation_tsz', '<=', Carbon::parse($request->get('toDate') . '23:59:59'));
            }
            $query = $query->select(array_merge([
                'receipts.*',
                'accounts.nickname',
                'accounts.status as account_status',
                'currency_rates.rate',
                DB::raw('FORMAT(receipts.total_price/currency_rates.rate,2) as total_price'),
                DB::raw('FORMAT(receipts.grandtotal/currency_rates.rate,2) as grandtotal'),
                DB::raw('FORMAT(receipts.subtotal/currency_rates.rate,2) as subtotal'),
                DB::raw('FORMAT(receipts.discount_amt/currency_rates.rate,2) as discount_amt'),
                DB::raw('FORMAT(receipts.gift_wrap_price/currency_rates.rate,2) as gift_wrap_price'),
                DB::raw('FORMAT(receipts.total_shipping_cost/currency_rates.rate,2) as total_shipping_cost'),
                DB::raw('FORMAT(receipts.total_vat_cost/currency_rates.rate,2) as total_vat_cost'),
                'shops.shop_name',
                DB::raw('CONCAT(accounts.nickname,"_",shops.shop_name) AS nickname_shop_name'),
                'receipt_shipments.carrier_name',
                'receipt_shipments.tracking_code',
                'receipt_shipments.tracking_url',
                'receipt_shipments.tracking_status',
                'receipts.country_iso AS iso_country_code',
                'receipts.supplier_order_total AS supplier_order_total',
                'shops.id as shop_id',
            ], User::selectCompanySupperAdmin($query)));

            if ($request->has('sortModel')) {
                foreach ($request->get('sortModel') as $col) {
                    $query = $query->orderByRaw("{$col['colId']} {$col['sort']}");
                }
            } else {
                $query = $query->orderByDesc('receipts.created_at');
            }

            if ($request->has('startRow') && $request->has('endRow')) {
                $startRow = intval($request->get('startRow'));
                $endRow = intval($request->get('endRow'));
                $perPage = $endRow - $startRow;

                $result = $query->paginate($perPage, ['*'], 'page', $startRow / $perPage + 1);

                return new AgGridResource($result);
            }

            return $query->get();
        }
    }

    public function show(Request $request, $id)
    {
        return DB::table('receipts')
            ->join('accounts', 'receipts.seller_user_id', '=', 'accounts.id')
            ->join('shops', 'shops.account_id', '=', 'accounts.id')
            ->leftJoin('receipt_shipments', 'receipts.id', '=', 'receipt_shipments.receipt_id')
            ->where('receipts.id', '=', $id)
            ->whereNull('receipts.deleted_at')
            ->select([
                'receipts.*',
                'accounts.nickname',
                'shops.shop_name',
                DB::raw('CONCAT(accounts.nickname,"_",shops.shop_name) AS nickname_shop_name'),
                'receipt_shipments.carrier_name',
                'receipt_shipments.tracking_code',
                'receipt_shipments.tracking_url',
                'receipt_shipments.tracking_status',
            ])
            ->first()
        ;
    }

    public function userShops()
    {
        $shopLists = Helper::getShopByUser(auth()->user());

        return array_map(function ($item) {
            return [
                'id' => $item['shop']['id'],
                'name' => $item['nickname'] . '_' . $item['shop']['shop_name'],
                'can_import' => $item['can_import'],
                'account_id' => $item['id'],
            ];
        }, $shopLists);
    }

    public function getTransactions($receiptId)
    {
        DB::enableQueryLog();

        $transactions = DB::table('transactions')
            ->join('accounts', 'transactions.seller_user_id', '=', 'accounts.id')
            ->join('company_teams', 'company_teams.id', '=', 'accounts.company_team_id')
            ->join('companies', 'companies.id', '=', 'company_teams.company_id')
            ->join('listings', 'transactions.listing_id', '=', 'listings.id')
//            ->join('listing_image', 'listing_image.listing_id', '=', 'listings.id')
            ->leftJoin('image_listings', 'image_listings.etsy_listing_image_id', '=', 'transactions.image_listing_id')
            ->leftJoin('currency_rates', 'transactions.currency_code', '=', 'currency_rates.currency_code')
            ->leftJoin('suppliers', 'transactions.supplier_id', '=', 'suppliers.id')
            ->leftJoin('supplier_variants', function ($join) {
                $join->on('transactions.supplier_variant_id', '=', 'supplier_variants.id')
                    ->join('supplier_catalogs', 'supplier_variants.supplier_catalog_id', '=', 'supplier_catalogs.id')
                ;
            })
            ->leftJoin('products', 'transactions.product_id', '=', 'products.id')
            ->leftJoin('product_attributes', 'product_attributes.product_id', '=', 'products.id')
            ->leftJoin('properties', 'product_attributes.property_id', '=', 'properties.id')
            ->leftJoin('property_scale', function ($join) {
                $join->on('property_scale.etsy_property_id', '=', 'properties.etsy_property_id')
                    ->where('properties.taxonomy_id', '=', 'listings.taxonomy_id')
                ;
            })
            ->leftJoin('scales', 'property_scale.scale_id', '=', 'scales.id')
            ->leftJoin('product_attribute_value', 'product_attribute_value.product_attribute_id', '=', 'product_attributes.id')
            ->leftJoin('values', 'product_attribute_value.value_id', '=', 'values.id')
            ->leftJoin('product_attributes as pa2', function ($join) {
                $join->on('pa2.product_id', '=', 'products.id')
                    ->whereColumn('product_attributes.property_id', '<>', 'pa2.property_id')
                ;
            })
            ->leftJoin('properties as p2', 'pa2.property_id', '=', 'p2.id')
            ->leftJoin('property_scale as ps2', function ($join) {
                $join->on('ps2.etsy_property_id', '=', 'p2.etsy_property_id')
                    ->where('p2.taxonomy_id', '=', 'listings.taxonomy_id')
                ;
            })
            ->leftJoin('scales as s2', 'ps2.scale_id', '=', 's2.id')
            ->leftJoin('product_attribute_value as pav2', 'pav2.product_attribute_id', '=', 'pa2.id')
            ->leftJoin('values as v2', 'pav2.value_id', '=', 'v2.id')
            ->where(function (Builder $query) {
                $query
                    ->whereNull('product_attributes.property_id')
                    ->orWhereNull('pa2.property_id')
                    ->orWhereColumn('product_attributes.property_id', '<', 'pa2.property_id')
                ;
            })
            ->where('transactions.receipt_id', '=', $receiptId)
            ->select([
                'transactions.id',
                'transactions.active',
                'transactions.receipt_id',
                'transactions.listing_id',
                'transactions.product_id',
                'transactions.url_dewix',
                'transactions.title',
                'transactions.quantity',
                'transactions.is_digital',
                'transactions.supplier_variant_id',
                'transactions.supplier_id',
                'transactions.to_supplier_order_id',
                'transactions.from_supplier_order_id',
                'transactions.shipping_service',
                'transactions.custom_design',
                'transactions.personalization',
                'transactions.supplier_status',
                'transactions.design_position as transaction_position',
                'transactions.is_import as is_import',
                DB::raw('FORMAT(transactions.price/currency_rates.rate,2) as price'),
                'listings.design_position as listing_position',
                DB::raw('IF(transactions.custom_design = 1, transactions.design_position, listings.design_position) AS design_position'),
                'listings.artwork_id',
                'image_listings.rank',
                'image_listings.url_75x75 as url_75x75',
                'image_listings.url_170x135 as url_170x135',
                'image_listings.url_fullxfull as url_fullxfull',
                'suppliers.name as supplier_name',
                'suppliers.icon as supplier_icon',
                'supplier_variants.size as variant_size',
                'supplier_variants.color as variant_color',
                'supplier_variants.display_name as supplier_variant_display_name',
                'supplier_catalogs.id as supplier_catalog_id',
                'supplier_catalogs.name as supplier_catalog_name',
                'supplier_catalogs.preset_id as supplier_catalog_preset_id',
                'supplier_catalogs.print_areas as supplier_catalog_print_areas',
                'products.sku',
                'properties.name as attr_first_name',
                'companies.id as company_id',
                'company_teams.id as company_team_id',
                DB::raw('GROUP_CONCAT(DISTINCT values.name) as attr_first_value'),
                'p2.name as attr_second_name',
                DB::raw('GROUP_CONCAT(DISTINCT v2.name) as attr_second_value'),
            ])
            ->groupBy([
                'transactions.id',
                'listings.design_position',
                'image_listings.rank',
                'image_listings.url_75x75',
                'image_listings.url_170x135',
                'image_listings.url_fullxfull',
                'suppliers.name',
                'suppliers.icon',
                'supplier_variants.supplier_catalog_id',
                'supplier_variants.size',
                'supplier_variants.color',
                'products.sku',
                'properties.name',
                'p2.name',
            ])
            ->get()
        ;

        if ($transactions->isEmpty()) {
            LogIgnore::debug('Transaction ' .$receiptId. ' is empty, query log: ', DB::getQueryLog());
        }

        foreach ($transactions as $transaction) {
            $transaction->designs = $this->getDesignDataFromTransaction($transaction);
        }

        return $transactions;
    }

    public function activeTransaction(Request $request, $id)
    {
        $transaction = Transaction::findOrFail($id);
        $transaction->active = $transaction->active ? false : true;
        $transaction->save();

        $isFull = $this->fullDesignStatus($transaction->receipt_id, true);

        return [
            'status' => 'OK',
            'data' => [
                'is_full' => $isFull,
                'receipt_id' => $transaction->receipt_id,
                'transaction_id' => $transaction->id,
                'active' => $transaction->active,
            ],
        ];
    }

    /**
     * @throws Throwable
     */
    public function detachedTransaction($id): JsonResponse
    {
        $transaction = Transaction::findOrFail($id);

        $quantity = $transaction->quantity;

        if ($quantity < 2) {
            return response()->json(
                'Quantity must be > 1',
                400
            );
        }

        DB::beginTransaction();

        try {
            $transaction->quantity = 1;
            $transaction->save();

            for ($i = 1; $i < $quantity; $i++) {
                $clonedTransaction = $transaction->replicate();
                $clonedTransaction->id = $id . '_' . $i;
                $clonedTransaction->quantity = 1;
                $clonedTransaction->save();
            }

            DB::commit();
        } catch (Throwable $e) {
            DB::rollBack();

            Log::error('Detached transactions error: ', [$e->getMessage(), $e->getLine(), $e->getFile(), $e->getCode()]);
            throw new Exception($e->getMessage());
        }

        return response()->json(true);
    }

    public function syncOrders(Request $request, $shopId)
    {
        $fromDate = empty($request->get('fromDate'))
            ? Carbon::now()->subDays(2)
            : Carbon::parse($request->get('fromDate') . ' 00:00:00')
        ;
        $toDate = empty($request->get('toDate'))
            ? Carbon::now()
            : Carbon::parse($request->get('toDate') . ' 23:59:59')
        ;

        $params = [];
        $params['min_created'] = $fromDate->setTimezone(self::US_LOS_ANGELES_TZ)->timestamp;
        $params['max_created'] = $toDate->setTimezone(self::US_LOS_ANGELES_TZ)->timestamp;

        SyncReceipt::dispatch($shopId, $params)->onQueue('etsy_receipt');

        return [
            'status' => 'OK',
            'message' => 'Sync is being processed',
        ];
    }

    public function getUploadUrl($transactionId, $position)
    {
        $transaction = Transaction::findOrFail($transactionId);
        if (!empty($transaction->supplier_status) && 'error' != $transaction->supplier_status) {
            abort(400, 'Supplier status is invalid .');
        }
        $filename = $this->genFilename($transaction, $position);

        return [
            'name' => $filename,
            'upload_url' => AwsS3Store::uploadPresignedUrl($filename),
        ];
    }

    public function getDesignUrl(Request $request)
    {
        $filename = $request->name;
        $cdnUrl = new CdnUrl($filename);

        return [
            'full_url' => AwsS3Store::cloudFrontFullUrl($filename),
            'thumb_url' => $cdnUrl->getThumbnail(200, 200, true),
        ];
    }

    public function setDesign(Request $request, $transactionId, $position)
    {
        $transaction = Transaction::findOrFail($transactionId);
        $listing = $transaction->listing;

        if (empty($listing->artwork_id)) {
            throw new Exception('Artwork ID is missing.', 1);
        }

        $request->validate([
            'full_url' => 'required|url',
            'thumb_url' => 'required|url',
        ], $request->all());

        $designMorphedModel = ${($transaction->custom_design ? 'transaction' : 'listing')};
        $designMorphedModel->designs()->updateOrCreate([
            'position' => $position,
        ], [
            'image_path' => null,
            'width' => $request->width,
            'height' => $request->height,
            'full_url' => $request->full_url,
            'thumb_url' => $request->thumb_url,
        ]);

        $isFull = $this->fullDesignStatus($transaction->receipt_id, true);
        $designData = $this->getDesignDataFromTransaction($transaction);

        return [
            'is_full' => $isFull,
            'designs' => $designData,
            'transaction_id' => $transactionId,
            'receipt_id' => $transaction->receipt_id,
        ];
    }

    public function designList(Request $request, $transactionId)
    {
        $transaction = Transaction::findOrFail($transactionId);
        if ($transaction->custom_design) {
            return $transaction->designs;
        }

        return $transaction->listing->designs;
    }

    public function deleteDesign(Request $request, $transactionId, $designId)
    {
        $transaction = Transaction::findOrFail($transactionId);

        if ($transaction->custom_design) {
            $designIds = $transaction->designs->pluck('id')->toArray();
        } else {
            $designIds = $transaction->listing->designs->pluck('id')->toArray();
        }

        if (!in_array($designId, $designIds)) {
            throw new Exception('Design ID is invalid.', 1);
        }

        $design = Design::findOrFail($designId);

        DB::beginTransaction();

        try {
            if (!empty($design->image_path)) {
                AwsS3Store::deleteFile([$design->image_path]);
            }
            $design->delete();

            $isFull = $this->fullDesignStatus($transaction->receipt_id, true);
            $designData = $this->getDesignDataFromTransaction($transaction);

            DB::commit();

            return [
                'is_full' => $isFull,
                'designs' => $designData,
                'transaction_id' => $transactionId,
                'receipt_id' => $transaction->receipt_id,
            ];
        } catch (Throwable $th) {
            DB::rollback();
            report($th);

            throw $th;
        }
    }

    public function updateSupplier(Request $request, $transactionId)
    {
        $supplierId = $request->get('supplier_id');
        if (!$request->has('supplier_id')) {
            abort(422, 'Missing parameters.');
        }

        $supplier = Supplier::findOrFail($supplierId);

        $transaction = Transaction::findOrFail($transactionId);
        $transaction->supplier_id = $supplier->id;
        $transaction->save();

        $isFull = $this->fullDesignStatus($transaction->receipt_id, true);

        return response()->json([
            'status' => 'OK',
            'data' => [
                'is_full' => $isFull,
                'supplier_id' => $supplier->id,
                'supplier_name' => $supplier->name,
                'transaction_id' => $transaction->id,
                'receipt_id' => $transaction->receipt_id,
            ],
        ]);
    }

    public function updateDetails(OrderDetailRequest $request, $receiptId)
    {
        $receipt = Receipt::findOrFail($receiptId);
        $receipt->name = $request->get('name');
        $receipt->buyer_email = $request->get('buyer_email') ?? null;
        $receipt->phone = $request->get('phone') ?: env('PHONE_NUMBER');
        $receipt->first_line = $request->get('first_line');
        $receipt->second_line = $request->get('second_line') ?? null;
        $receipt->city = $request->get('city');
        $receipt->state = $request->get('state');
        $receipt->zip = $request->get('zip');
        $receipt->country_iso = $request->get('iso_country_code');
        $receipt->message_from_buyer = $request->get('message_from_buyer') ?? null;
        $receipt->gift_message = $request->get('gift_message') ?? null;
        $receipt->ioss_number = $request->get('ioss_number') ?? null;
        $receipt->ioss_number_total = $request->get('ioss_number_total') ?? null;
        $receipt->tax_number = $request->get('tax_number') ?? null;
        $receipt->tax_type = $request->get('tax_type') ?? null;
        $receipt->order_tax_value = $request->get('order_tax_value') ?? null;
        $receipt->team_message = $request->get('team_message') ?? null;

        $secondLine = !empty($receipt->second_line) ? "\n" . $receipt->second_line : '';
        $cityStateZip = Str::upper("{$request->get('city')}, {$request->get('state')} {$request->get('zip')}");

        $receipt->formatted_address = <<<EOT
{$request->get('name')}
{$request->get('first_line')}{$secondLine}
{$cityStateZip}
{$request->get('iso_country_code')}
EOT;

        $lineItems = $receipt->line_items;
        if (empty($lineItems)
            || (
                isset($lineItems[0]['address_verified'])
                && !$lineItems[0]['address_verified']
            )
        ) {
            // Check address if order invalid address
            $smartyService = new SmartyApi();
            try {
                // Check address
                if ($request->get('iso_country_code') === 'US') {
                    $addressCheck = $smartyService->usAddressVerify([
                        'street' => $request->get('first_line'),
                        'street2' => $request->get('second_line'),
                        'city' => $request->get('city'),
                        'state' => $request->get('state'),
                        'zipcode' => $request->get('zip'),
                    ]);
                } else {
                    $addressCheck = $smartyService->internationAddressVerify([
                        'country' => $request->get('iso_country_code'),
                        'address1' => $request->get('first_line'),
                        'address2' => $request->get('second_line'),
                        'locality' => $request->get('city'),
                        'administrative_area' => $request->get('state'),
                        'postal_code' => $request->get('zip'),
                    ]);
                }
            } catch (Exception $e) {
                $addressCheck = [
                    'verified' => false,
                    'notes' => 'Network or credentials error!'
                ];

                Log::error('Check address error: ', [
                    $e->getMessage(),
                    $e->getLine(),
                    $e->getFile()
                ]);
            }

            if (! empty($lineItems)) {
                $lineItems = $lineItems[0];
                $lineItems['address_verified'] = $addressCheck['verified'];
                $lineItems['address_verified_notes'] = $addressCheck['notes'];
            } else {
                $lineItems = [
                    "error_message" => null,
                    "supplier_name" => null,
                    "supplier_status" => null,
                    "supplier_item_date" => null,
                    "to_supplier_order_id" => null,
                    "from_supplier_order_id" => null,
                    "address_verified" => $addressCheck['verified'],
                    "address_verified_notes" => $addressCheck['notes'],
                ];
            }

            $receipt->line_items = [$lineItems];

            // Send message to Lark group
            $larkMessageService = new LarkMessageApi();

            if (isset($addressCheck['verified']) && $addressCheck['verified']) {
                $message = 'Order ' . $receipt->id . '. Address VERIFIED by Smarty!';
                try {
                    $larkMessageService->send($message);
                } catch (Exception $e) {
                }
            } else {
                $note = str_replace(PHP_EOL, ', ', $addressCheck['notes']);
                $message = 'Error: ' . $note;
                try {
                    $larkMessageService->sendRich('INVALID ADDRESS - ' . $receipt->id, $message, [
                        'text' => 'Please check',
                        'uri' => env('APP_URL') . '/orders?orderId=' . $receipt->id
                    ]);
                } catch (Exception $e) {
                }
            }
        }

        $receipt->save();

        PushOrderToElasticJob::dispatchNow($receipt->id);

        return response()->json([
            'status' => 'OK',
            'data' => [
                'formatted_address' => $receipt->formatted_address,
                'receipt_id' => $receipt->id,
            ],
        ]);
    }

    public function setDesignPosition(Request $request, $transactionId)
    {
        $designPosition = $request->get('design_position');
        if (!$request->has('design_position')) {
            abort(422, 'Missing parameters.');
        }

        $transaction = Transaction::findOrFail($transactionId);
        if ($transaction->custom_design) {
            $transaction->design_position = $designPosition;
            $transaction->save();
        } else {
            $listing = $transaction->listing;
            $listing->design_position = $designPosition;
            $listing->save();
        }

        $isFull = $this->fullDesignStatus($transaction->receipt_id, true);
        $designData = $this->getDesignDataFromTransaction($transaction);
        $transactionFollows = $this->syncFullDesign($transaction);

        return response()->json([
            'status' => 'OK',
            'data' => [
                'is_full' => $isFull,
                'design_position' => $designPosition,
                'listing_id' => $transaction->listing_id,
                'transaction_id' => $transaction->id,
                'receipt_id' => $transaction->receipt_id,
                'designs' => $designData,
                'transaction_follows' => $transactionFollows,
            ],
        ]);
    }

    public function updateQuantity(Request $request, $transactionId)
    {
        $validator = Validator::make($request->all(), [
            'quantity' => 'required|integer|min:1',
        ]);
        if ($validator->fails()) {
            foreach ($validator->messages()->getMessages() as $message) {
                throw new Exception($message[0], 1);
            }

            return false;
        }
        $quantity = $request->get('quantity');
        $transaction = Transaction::findOrFail($transactionId);
        $transaction->quantity = $quantity;
        $transaction->save();

        return response()->json([
            'status' => 'OK',
            'data' => [
                'quantity' => $quantity,
                'transaction_id' => $transaction->id,
            ],
        ]);
    }

    public function customDesign(Request $request, $transactionId)
    {
        $customDesign = $request->get('custom_design');
        if (!$request->has('custom_design')) {
            abort(422, 'Missing parameters.');
        }

        $transaction = Transaction::findOrFail($transactionId);
        $transaction->custom_design = (int) $customDesign;
        if ($transaction->custom_design && empty($transaction->design_position)) {
            $transaction->design_position = $transaction->listing->design_position;
        }
        $transaction->save();

        $isFull = $this->fullDesignStatus($transaction->receipt_id, true);
        $designData = $this->getDesignDataFromTransaction($transaction);

        return response()->json([
            'status' => 'OK',
            'data' => [
                'is_full' => $isFull,
                'custom_design' => $transaction->custom_design,
                'transaction_id' => $transaction->id,
                'receipt_id' => $transaction->receipt_id,
                'designs' => $designData,
            ],
        ]);
    }

    public function setSupplierVariant(Request $request, $transactionId)
    {
        $transaction = Transaction::findOrFail($transactionId);
        $addingData = [];

        if ('DELETE' == $request->getMethod()) {
            $supplierVariantId = null;
        } else {
            $supplierVariantId = $request->get('supplier_variant_id');
            if (!$request->has('supplier_variant_id')) {
                abort(422, 'Missing parameters.');
            }
            $supplierVariant = SupplierVariant::findOrFail($supplierVariantId);

            /** @var Supplier $supplier */
            $supplier = $supplierVariant->catalog->supplier()->firstOrFail();
            if ($transaction->supplier_id != $supplier->id) {
                abort(400, 'Supplier variant does not match with supplier.');
            }

            $transactionFollows = $this->syncSupplierVariant($transaction->product_id, $transaction->id, $supplier, $supplierVariantId, $transaction->shipping_service);

            $addingData = [
                'supplier' => $supplier->name,
                'supplier_variant_id' => $supplierVariant->id,
                'supplier_variant_name' => $supplierVariant->name,
                'variant_size' => $supplierVariant->size,
                'variant_color' => $supplierVariant->color,
                'transaction_follows' => $transactionFollows,
            ];
        }

        $transaction->supplier_variant_id = $supplierVariantId;
        $transaction->save();

        $isFull = $this->fullDesignStatus($transaction->receipt_id, true);

        return response()->json([
            'status' => 'OK',
            'data' => array_merge([
                'is_full' => $isFull,
                'transaction_id' => $transaction->id,
                'receipt_id' => $transaction->receipt_id,
            ], $addingData),
        ]);
    }

    public function updateShippingService(Request $request, $transactionId)
    {
        $transaction = Transaction::findOrFail($transactionId);
        $validator = Validator::make($request->all(), [
            'shipping_service' => ['present', new ShippingService()],
        ]);
        if ($validator->fails()) {
            foreach ($validator->messages()->getMessages() as $message) {
                throw new Exception($message[0], 1);
            }

            return false;
        }
        $transaction->shipping_service = $request->get('shipping_service');
        $transaction->save();
        $isFull = $this->fullDesignStatus($transaction->receipt_id, true);

        return response()->json([
            'status' => 'OK',
            'data' => [
                'is_full' => $isFull,
                'shipping_service' => $request->get('shipping_service'),
                'transaction_id' => $transaction->id,
                'receipt_id' => $transaction->receipt_id,
            ],
        ]);
    }

    public function destroy(Request $request)
    {
        $ids = $request->get('ids');
        if (!$request->has('ids')) {
            abort(422, 'Missing parameters.');
        }

        DB::beginTransaction();

        try {
            Transaction::whereIn('receipt_id', $ids)->delete();
            Receipt::whereIn('id', $ids)->delete();

            DB::commit();
        } catch (Throwable $th) {
            DB::rollback();

            throw $th;
        }

        return null;
    }

    public function cloneOrder($id, Request $request)
    {
        if (empty($request->transactionClones)) {
            return response(['message' => 'Transaction to clone is empty  .'], 400);
        }
        $order = Receipt::findOrFail($id);
        $newOrder = $order->replicate();
        $cloneId = null;
        $orderIdMain = explode('_omz', $order->id);

        $preCloneId = 1;
        do {
            $cloneId = $orderIdMain[0] . '_omz' . $preCloneId;
            ++$preCloneId;
        } while (DB::table('receipts')->where('id', $cloneId)->exists());

        $newOrder->id = $cloneId;
        $newOrder->creation_tsz = Carbon::now();
        $newOrder->created_at = Carbon::now();
        $newOrder->was_shipped = 0;
        $newOrder->is_dead = 0;
        $newOrder->is_overdue = 0;
        $newOrder->shipped_date = null;
        $newOrder->line_items = null;
        $newOrder->supplier_order_total = null;
        $newOrder->supplier_shipping_total = null;
        $newOrder->is_shipped = 0;

        $cloneTransactions = [];
        foreach ($order->transactions as $key => $transaction) {
            if (!in_array($transaction->id, $request->transactionClones)) {
                continue;
            }
            $newTransaction = $transaction->replicate();
            $transactionId = explode('_', $transaction->id);
            $newTransaction->id = $transactionId[0] . '_' . Str::random(8) . $preCloneId . $key;
            $newTransaction->creation_tsz = Carbon::now();
            $newTransaction->created_at = Carbon::now();
            $newTransaction->to_supplier_order_id = null;
            $newTransaction->from_supplier_order_id = null;
            $newTransaction->supplier_status = null;
            $newTransaction->carrier_name = null;
            $newTransaction->tracking_code = null;
            $newTransaction->tracking_url = null;
            $newTransaction->tracking_status = null;
            $designs = [];
            foreach ($transaction->designs as $design) {
                $newDesign = $design->replicate();
                $newDesign->created_at = Carbon::now();
                $designs[] = $newDesign;
            }
            $cloneTransactions[] = [
                'transactions' => $newTransaction,
                'designs' => $designs,
            ];
        }
        DB::beginTransaction();

        try {
            $newOrder->save();

            PushOrderToElasticJob::dispatchNow($newOrder->id);

            foreach ($cloneTransactions as $key => $item) {
                $cloneTransaction = $item['transactions'];
                $cloneTransaction->receipt_id = $newOrder->id;
                $cloneTransaction->save();
                foreach ($item['designs'] as $design) {
                    $design->designable_id = $cloneTransaction->id;
                    $design->save();
                }
            }

            $this->fullDesignStatus($newOrder, true);
            DB::commit();

            return response(['message' => $newOrder->id . ' is created .']);
        } catch (Exception $th) {
            DB::rollBack();
            report($th);

            return response(['message' => 'Clone is failed .'], 409);
        }
    }

    public function productSupplier(ProductSupplierRequest $request, $transactionId)
    {
        try {
            $requestPosition = empty($request->designs) ? null : implode(',', array_keys($request->designs));

            $supplier = Supplier::findOrFail($request->supplier_id);
            $supplierVariant = SupplierVariant::findOrFail($request->supplier_variant_id);
            if (
                'gearment' == $supplier->name
                && 'whole,both' == $supplierVariant->catalog->print_areas
                && !empty($requestPosition)
            ) {
                $requestPosition = 'front' == $requestPosition ? 'whole' : 'both';
            }

            $printAreasArray = explode(',', $supplierVariant->catalog->print_areas);
            $requestPositionArray = explode(',', $requestPosition);
            $invalidPositionArray = array_diff($requestPositionArray, $printAreasArray);
            if (!empty($invalidPositionArray)) {
                abort(400, 'Print Area is invalid: ' . implode(',', $invalidPositionArray));
            }

            $transaction = Transaction::findOrFail($transactionId);
            if (!empty($transaction->supplier_status) && 'error' != $transaction->supplier_status) {
                abort(400, 'Supplier status is invalid.');
            }

            $listing = $transaction->listing;

            $transaction->custom_design = boolval('printify' == $supplier->name ? true : $request->custom_design);
            $transaction->supplier_id = $request->supplier_id;
            $transaction->supplier_variant_id = $request->supplier_variant_id;
            $transaction->shipping_service = $request->shipping_service;
            if ($transaction->custom_design) {
                $transaction->design_position = $requestPosition;
            } else {
                $listing->design_position = $requestPosition;
                $listing->save();
            }
            $transaction->save();

            $designMorphedModel = ${($transaction->custom_design ? 'transaction' : 'listing')};
            DB::table('designs')->whereIn('id', $designMorphedModel->designs->pluck('id')->toArray())->delete();
            foreach ($request->designs ?? [] as $key => $value) {
                if (empty($value)) {
                    continue;
                }

                $designMorphedModel->designs()->create([
                    'position' => $key,
                    'image_path' => $value['name'],
                    'height' => $value['height'],
                    'width' => $value['width'],
                    'full_url' => $value['full_url'],
                    'thumb_url' => $value['thumb_url'],
                ]);
            }

            $isFull = $this->fullDesignStatus($transaction->receipts, true);
            $designData = $this->getDesignDataFromTransaction($transaction);

            $receiptFollows = [];
            if (!empty($request->visible_receipt_ids)) {
                /** @var Receipt $visibleReceipt */
                foreach (Receipt::whereIn('id', $request->visible_receipt_ids)->get() as $visibleReceipt) {
                    $listingIds = $visibleReceipt->transactions->pluck('listing_id')->toArray();
                    if (!in_array($transaction->listing_id, $listingIds)) {
                        continue;
                    }
                    $receiptFollows[$visibleReceipt->id]['is_full'] = $this->fullDesignStatus($visibleReceipt, true);
                }
            }

            $transactionFollows = [];
            if (!empty($request->visible_transaction_ids)) {
                DB::table('transactions')
                    ->whereNull(['supplier_variant_id', 'supplier_id'])
                    ->where('id', '<>', $transactionId)
                    ->where('product_id', '=', $transaction->product_id)
                    ->update([
                        'supplier_id' => $transaction->supplier_id,
                        'supplier_variant_id' => $transaction->supplier_variant_id,
                        'shipping_service' => $transaction->shipping_service,
                    ])
            ;

                /** @var Transaction $visibleTransaction */
                foreach (Transaction::whereIn('id', $request->visible_transaction_ids)->get() as $visibleTransaction) {
                    if (
                    $visibleTransaction->id == $transactionId
                    || $visibleTransaction->listing_id != $transaction->listing_id
                ) {
                        continue;
                    }

                    $temp = [
                        'receipt_id' => $visibleTransaction->receipt_id,
                        'transaction_id' => $visibleTransaction->id,
                        'supplier_id' => $visibleTransaction->supplier_id,
                        'supplier_catalog_id' => $visibleTransaction->supplier_variant ? $visibleTransaction->supplier_variant->supplier_catalog_id : null,
                        'supplier_variant_id' => $visibleTransaction->supplier_variant_id,
                        'shipping_service' => $visibleTransaction->shipping_service,
                        'custom_design' => $visibleTransaction->custom_design,
                        'is_full' => $this->fullDesignStatus($visibleTransaction->receipt_id, true),
                    ];
                    if ($visibleTransaction->listing_id == $transaction->listing_id && !$visibleTransaction->custom_design) {
                        $temp['design_position'] = $transaction->listing->design_position;
                        $temp['designs'] = $this->getDesignDataFromTransaction($visibleTransaction);
                    }
                    $transactionFollows[$visibleTransaction->id] = $temp;
                }
            }

            try {
                ListingHistory::updateOrCreate(
                    [
                        'listing_id' => $request->get('listing_id'),
                    ],
                    [
                        'supplier_suggestion' => [
                            'transaction_id' => $transaction->id,
                            'supplier_id' => $request->get('supplier_id'),
                            'supplier_catalog_id' => $request->get('supplier_catalog_id'),
                            'supplier_variant_id' => $request->get('supplier_variant_id'),
                            'shipping_service' => $request->get('shipping_service'),
                            'custom_design' => $transaction->custom_design,
                            'design_position' => $transaction->custom_design ? $transaction->design_position : $listing->design_position,
                            'designs' => $designData,
                        ]
                    ]
                );
            } catch (Throwable $e) {
                Log::error('Save listing history error', [$e->getMessage(), $e->getLine(), $e->getFile()]);
            }

            return response()->json([
                'status' => 'OK',
                'data' => [
                    'receipt_id' => $transaction->receipt_id,
                    'transaction_id' => $transaction->id,
                    'supplier_id' => $transaction->supplier_id,
                    'supplier_variant_id' => $transaction->supplier_variant_id,
                    'shipping_service' => $transaction->shipping_service,
                    'custom_design' => $transaction->custom_design,
                    'design_position' => $transaction->custom_design ? $transaction->design_position : $listing->design_position,
                    'is_full' => $isFull,
                    'designs' => $designData,
                    'receipt_follows' => $receiptFollows,
                    'transaction_follows' => $transactionFollows,
                ],
            ]);
        } catch (Exception $e) {
            abort(400, $e->getMessage());
        }
    }

    public function merge(Request $request)
    {
        $request->validate([
            'receipt_ids' => 'required|exists:receipts,id',
        ], $request->all());

        $mergeId = null;
        $receiptIds = $request->receipt_ids;

        if (DB::table('transactions')
            ->whereIn('receipt_id', $receiptIds)
            ->whereNotNull('supplier_status')
            ->exists()) {
            throw new Exception('Do not select fulfilled order', 1);
        }
        if (1 != DB::table('transactions')
            ->whereIn('receipt_id', $receiptIds)
            ->select(['transactions.seller_user_id'])
            ->distinct()
            ->get()->count()) {
            throw new Exception('Merge must be same account .', 1);
        }

        foreach ($receiptIds as $receiptId) {
            $mergeId = $receiptId;
            if (is_numeric($mergeId)) {
                break;
            }
        }
        $removeIds = array_values(array_diff($receiptIds, [$mergeId]));

        DB::transaction(function () use ($mergeId, $removeIds) {
            foreach ($removeIds as $removeId) {
                $transactions = DB::table('transactions')->where('receipt_id', (string) $removeId)->get(['id', 'merge_receipt_id']);
                foreach ($transactions as $transaction) {
                    if (empty($transaction->merge_receipt_id)) {
                        $transaction->merge_receipt_id = $removeId;
                    }
                    DB::table('transactions')->where('id', $transaction->id)
                        ->update([
                            'receipt_id' => $mergeId,
                            'merge_receipt_id' => $transaction->merge_receipt_id,
                        ])
                    ;
                }

                Receipt::where('id', $removeId)->delete();
            }
        });

        $isFull = $this->fullDesignStatus($mergeId, true);

        return [
            'status' => 'OK',
            'data' => [
                'merge' => $this->show($request, $mergeId),
                'remove' => $removeIds,
                'receipt_id' => $mergeId,
                'is_full' => $isFull,
            ],
        ];
    }

    public function resetOrder(Request $request)
    {
        if (!isset($request->orderIds)) {
            abort(400, 'orderIds is required .');
        }

        $orderIds = $request->orderIds;

        try {
            Transaction::whereIn('receipt_id', $orderIds)
                ->update([
                    'to_supplier_order_id' => null,
                    'from_supplier_order_id' => null,
                    'supplier_status' => null,
                    'carrier_name' => null,
                    'tracking_code' => null,
                    'tracking_url' => null,
                    'tracking_status' => null,
                ])
            ;
            Receipt::whereIn('id', $orderIds)->update(['line_items' => null]);

            foreach ($orderIds as $id) {
                PushOrderToElasticJob::dispatchNow($id);
            }

            return [
                'status' => 'OK',
            ];
        } catch (Throwable $th) {
            abort($th->getMessage());
        }
    }

    public function reSync(Request $request, $receiptId)
    {
        if (!is_numeric($receiptId)) {
            throw new Exception('Cannot resync copies of orders', 1);
        }
        $receipt = Receipt::findOrFail($receiptId);
        $account = $receipt->account;

        $etsyOauth = new EtsyV3Oauth($account);
        $response = $etsyOauth->get('/shops/' . $account->shop->id . '/receipts/' . $receipt->id);
        $userId = auth()->user()->id;
        $keyNotify = $userId . $account->id . now() . rand(10000, 99999);
        Redis::hset('count_sync_receipt_queue', $keyNotify, 1);
        SaveReceipt::dispatch($account, $response, $userId, $keyNotify, true)->onQueue('etsy_receipt');

        Artisan::call('sync:supplier-order ' . $receiptId);

        return response()->json(['status' => 'OK'], 202);
    }

    public function cancel(Request $request, $receiptId)
    {
        $receipt = Receipt::findOrFail($receiptId);
        $receipt->update(['status' => 'canceled']);

        return response()->json(['status' => 'OK']);
    }

    public function setTrackingStatus(Request $request)
    {
        if (empty($request->tracking_status) || empty($request->ids)) {
            abort(404, 'Tracking Status Or Ids is empty .');
        }
        foreach ($request->ids as $id) {
            $transactions = DB::table('transactions')
                ->join('suppliers', 'transactions.supplier_id', '=', 'suppliers.id')
                ->where('transactions.receipt_id', '=', $id)
                ->select([
                    'transactions.*',
                    'suppliers.name as supplier_name',
                ])
                ->get()
            ;

            $updatedData = [];
            $suppliers = [];
            foreach ($transactions as $transaction) {
                if (!in_array($transaction->supplier_name, $suppliers)) {
                    $updatedData[] = [
                        'supplier_name' => $transaction->supplier_name,
                        'supplier_status' => $transaction->supplier_status,
                        'to_supplier_order_id' => $transaction->to_supplier_order_id,
                        'from_supplier_order_id' => $transaction->from_supplier_order_id,
                        'error_message' => $transaction->error_message,
                        'tracking_status' => $request->get('tracking_status'),
                        'carrier_name' => $transaction->carrier_name,
                        'tracking_code' => $transaction->tracking_code,
                        'tracking_url' => $transaction->tracking_url,
                    ];
                    $suppliers[] = $transaction->supplier_name;
                }
            }
            DB::beginTransaction();

            try {
                $transactions = DB::table('transactions')
                    ->where('transactions.receipt_id', '=', $id)
                    ->update(['tracking_status' => $request->get('tracking_status')])
                ;

                Receipt::where('id', $id)->update(
                    [
                        'line_items' => $updatedData,
                        'is_shipped' => true,
                    ]
                );

                DB::commit();
            } catch (Throwable $th) {
                DB::rollback();
                abort(404, $th->getMessage());
            }
        }

        return 0;
    }

    public function getTracking(Request $request)
    {
        if (empty($request->get_trackings)) {
            abort(400, 'Tracking supplier is invalid .');
        }

        try {
            foreach ($request->get_trackings as $tracking) {
                if (empty($tracking['from_supplier_order_id'])) {
                    abort(400, 'Supplier ID is invalid .');
                }
                if (!in_array($tracking['supplier_name'], ['geargag', 'gearment', 'printify', 'customcat', 'dreamship', 'printway', 'sellerwix', 'swiftpod'])) {
                    abort(400, 'Supplier  is invalid .');
                }
                Artisan::call('get:tracking ' . $tracking['supplier_name'] . ' --id=' . $tracking['from_supplier_order_id'] . ' --error');
            }

            return response([
                'status' => 'ok',
            ]);
        } catch (Throwable $e) {
            abort(400, $e->getMessage());
        }
    }

    public function shippingMethods()
    {
        return response(config('shipping_methods'));
    }

    protected function genFilename($transaction, $position)
    {
        /** @var Listing $listing */
        $listing = $transaction->listing;

        $filename = '';
        if ($transaction->custom_design) {
            $filename .= 'transaction_' . $transaction->id;
        } else {
            $filename .= 'listing_' . $listing->etsy_listing_id;
        }
        $filename .= uniqid('_' . $position . '_' . time());
        $filename .= '.png';

        if (Design::whereImagePath($filename)->exists()) {
            $filename = $this->genFilename($transaction, $position);
        }

        return $filename;
    }
}
