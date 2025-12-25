/**
 * ProductCreator Reducer
 * Manages complex state for ProductCreator component
 */

export const initialState = {
  searchMode: 'comics',
  showCreateForm: false,
  saving: false,
  message: null,
  detailsLoading: false,
  
  // Comics
  comics: {
    params: { series: '', number: '', publisher: '', upc: '' },
    results: [],
    loading: false,
    selected: null
  },
  
  // Funkos
  funkos: {
    query: '',
    seriesFilter: '',
    results: [],
    loading: false,
    page: 1,
    totalPages: 1,
    total: 0,
    stats: null
  },
  
  // BCW
  bcw: {
    query: '',
    categoryFilter: '',
    results: [],
    loading: false,
    page: 1,
    totalPages: 1,
    total: 0,
    categories: []
  },
  
  // Form
  form: {
    sku: '', name: '', description: '', category: 'comics', subcategory: '',
    price: '', original_price: '', stock: 1, image_url: '',
    issue_number: '', publisher: '', year: '', upc: '', featured: false, tags: [],
    variant: '',
    grading_company: 'cgc',
    certification_number: '',
    cgc_grade: '',
    grade_label: 'universal',
    is_graded: false,
    // Case Intelligence (New)
    case_quantity: '',
    case_weight: '',
    case_dimensions: ''
  }
};

export function reducer(state, action) {
  switch (action.type) {
    case 'SET_SEARCH_MODE':
      return { ...state, searchMode: action.payload };
    case 'TOGGLE_CREATE_FORM':
      return { ...state, showCreateForm: action.payload };
    case 'SET_SAVING':
      return { ...state, saving: action.payload };
    case 'SET_MESSAGE':
      return { ...state, message: action.payload };
    case 'SET_DETAILS_LOADING':
      return { ...state, detailsLoading: action.payload };
      
    // Comics Actions
    case 'UPDATE_COMIC_PARAMS':
      return { ...state, comics: { ...state.comics, params: { ...state.comics.params, ...action.payload } } };
    case 'SET_COMIC_RESULTS':
      return { ...state, comics: { ...state.comics, results: action.payload, loading: false } };
    case 'SET_COMIC_LOADING':
      return { ...state, comics: { ...state.comics, loading: action.payload } };
    case 'SET_SELECTED_COMIC':
      return { ...state, comics: { ...state.comics, selected: action.payload } };
      
    // Funko Actions
    case 'UPDATE_FUNKO_SEARCH':
      return { ...state, funkos: { ...state.funkos, ...action.payload } };
    case 'SET_FUNKO_RESULTS':
      return { ...state, funkos: { ...state.funkos, ...action.payload, loading: false } };
    case 'SET_FUNKO_STATS':
      return { ...state, funkos: { ...state.funkos, stats: action.payload } };
      
    // BCW Actions
    case 'UPDATE_BCW_SEARCH':
      return { ...state, bcw: { ...state.bcw, ...action.payload } };
    case 'SET_BCW_RESULTS':
      return { ...state, bcw: { ...state.bcw, ...action.payload, loading: false } };
    case 'SET_BCW_CATEGORIES':
      return { ...state, bcw: { ...state.bcw, categories: action.payload } };
      
    // Form Actions
    case 'UPDATE_FORM':
      return { ...state, form: { ...state.form, ...action.payload } };
    case 'RESET_FORM':
      return { ...state, form: initialState.form, showCreateForm: false };
      
    case 'CLEAR_ALL':
      return { 
        ...initialState, 
        searchMode: state.searchMode,
        funkos: { ...initialState.funkos, stats: state.funkos.stats },
        bcw: { ...initialState.bcw, categories: state.bcw.categories }
      };
      
    default:
      return state;
  }
}
